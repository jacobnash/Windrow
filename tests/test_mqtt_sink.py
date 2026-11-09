import json
import os
import tempfile

import pytest

from windrow import mqtt_sink


class FakeClient:
    def __init__(self, connected=True):
        self._connected = connected
        self.published = []

    def is_connected(self):
        return self._connected

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload), qos, retain))


@pytest.fixture(autouse=True)
def spool_file(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "spool.jsonl")
        monkeypatch.setenv("WINDROW_SPOOL_FILE", path)
        yield path


def test_connected_publish_never_touches_spool(spool_file):
    client = FakeClient(connected=True)

    mqtt_sink.publish_reading(client, "fbf/site-a", "zone-temp", 72.5, ts=100.0)

    assert client.published == [("fbf/site-a/zone-temp", {"point": "zone-temp", "value": 72.5, "ts": 100.0}, 1, True)]
    assert not os.path.exists(spool_file)


def test_disconnected_publish_appends_to_spool(spool_file):
    client = FakeClient(connected=False)

    mqtt_sink.publish_reading(client, "fbf/site-a", "zone-temp", 72.5, ts=100.0)

    assert client.published == []
    records = mqtt_sink._read_spool(spool_file)
    assert records == [{"topic_prefix": "fbf/site-a", "point": "zone-temp", "value": 72.5, "ts": 100.0}]


def test_cap_eviction_drops_oldest(spool_file, monkeypatch):
    monkeypatch.setattr(mqtt_sink, "MAX_SPOOL_ROWS", 3)
    client = FakeClient(connected=False)

    for i in range(5):
        mqtt_sink.publish_reading(client, "fbf/site-a", "zone-temp", i, ts=float(i))

    records = mqtt_sink._read_spool(spool_file)
    assert [r["ts"] for r in records] == [2.0, 3.0, 4.0]


def test_corrupt_trailing_line_does_not_lose_prior_lines(spool_file):
    with open(spool_file, "w") as f:
        f.write(json.dumps({"topic_prefix": "fbf/site-a", "point": "p1", "value": 1, "ts": 1.0}) + "\n")
        f.write("{not valid json")

    records = mqtt_sink._read_spool(spool_file)
    assert records == [{"topic_prefix": "fbf/site-a", "point": "p1", "value": 1, "ts": 1.0}]


def test_on_connect_flushes_spool_preserving_original_ts(spool_file):
    client = FakeClient(connected=False)
    mqtt_sink.publish_reading(client, "fbf/site-a", "zone-temp", 72.5, ts=100.0)
    mqtt_sink.publish_reading(client, "fbf/site-a", "damper-pos", 40, ts=101.0)

    client._connected = True
    mqtt_sink._on_connect(client, None, None, 0, None)

    assert client.published == [
        ("fbf/site-a/zone-temp", {"point": "zone-temp", "value": 72.5, "ts": 100.0}, 1, True),
        ("fbf/site-a/damper-pos", {"point": "damper-pos", "value": 40, "ts": 101.0}, 1, True),
    ]
    assert not os.path.exists(spool_file)


def test_flush_stops_and_leaves_spool_untouched_if_disconnected_mid_replay():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "spool.jsonl")
        os.environ["WINDROW_SPOOL_FILE"] = path
        try:
            client = FakeClient(connected=False)
            mqtt_sink.publish_reading(client, "fbf/site-a", "p1", 1, ts=1.0)
            mqtt_sink.publish_reading(client, "fbf/site-a", "p2", 2, ts=2.0)

            class FlakyClient(FakeClient):
                def is_connected(self):
                    # Connected for the first record, disconnected before
                    # the second - simulates the link dropping mid-replay.
                    was_connected = self._connected
                    self._connected = False
                    return was_connected

            flaky = FlakyClient(connected=True)
            mqtt_sink._on_connect(flaky, None, None, 0, None)

            # The one record published before the drop is a harmless
            # at-least-once duplicate next flush - the file itself is left
            # fully untouched (not rewritten to drop what already went
            # out), which is the deliberate simplification the plan calls
            # for instead of partial-rewrite bookkeeping.
            assert len(flaky.published) == 1
            assert os.path.exists(path)
            records = mqtt_sink._read_spool(path)
            assert len(records) == 2
        finally:
            del os.environ["WINDROW_SPOOL_FILE"]


def test_tls_on_for_8883_or_when_asked(monkeypatch):
    """Edge nodes publish to Baler over MQTT/TLS on 8883 (Baler#36)."""
    from windrow.mqtt_sink import use_tls

    monkeypatch.delenv("MQTT_TLS", raising=False)
    assert use_tls(8883) is True
    assert use_tls(1883) is False
    assert use_tls(1883, tls=True) is True
    assert use_tls(8883, tls=False) is False
    monkeypatch.setenv("MQTT_TLS", "1")
    assert use_tls(1883) is True
    monkeypatch.setenv("MQTT_TLS", "0")
    assert use_tls(8883) is False


# --- the batched envelope (Baler#234) ----------------------------------------


@pytest.fixture
def batch_mode(monkeypatch):
    monkeypatch.setattr(mqtt_sink, "ENVELOPE", "batch")
    monkeypatch.setattr(mqtt_sink, "BATCH_WINDOW_S", 60.0)  # flushed explicitly in these tests
    monkeypatch.setattr(mqtt_sink, "_batches", mqtt_sink._Batches())
    yield
    mqtt_sink._batches.flush()


def test_the_per_point_envelope_is_the_default():
    assert mqtt_sink.ENVELOPE == "point"
    client = FakeClient()
    assert mqtt_sink.publish_live(client, "fbf/site-a", "zone-temp", 72.5, ts=100.0) == "fbf/site-a/zone-temp"
    assert client.published == [("fbf/site-a/zone-temp", {"point": "zone-temp", "value": 72.5, "ts": 100.0}, 1, True)]


def test_a_poll_tick_is_one_message_not_retained(batch_mode):
    client = FakeClient()
    for label, value in (("zone-temp", 72.5), ("damper-pos", 40), ("fan", "active")):
        assert mqtt_sink.publish_live(client, "fbf/site-a/ahu-1", label, value, ts=100.0) == "fbf/site-a/ahu-1/batch"
    mqtt_sink.publish_live(client, "fbf/site-a/ahu-2", "zone-temp", 70.0, ts=100.0)
    assert client.published == []  # waiting for the window or the tick's end
    assert mqtt_sink.flush_batches("fbf/site-a/ahu-1") == 1
    assert client.published == [("fbf/site-a/ahu-1/batch",
                                 {"ts": 100.0, "points": {"zone-temp": 72.5, "damper-pos": 40, "fan": "active"}}, 1, False)]
    mqtt_sink.flush_batches()
    assert client.published[-1] == ("fbf/site-a/ahu-2/batch", {"ts": 100.0, "points": {"zone-temp": 70.0}}, 1, False)
    assert mqtt_sink.batch_stats()["messages"] == 2 and mqtt_sink.batch_stats()["readings"] == 4


def test_differing_timestamps_and_repeats_use_the_list_form(batch_mode):
    client = FakeClient()
    mqtt_sink.publish_live(client, "fbf/s", "a", 1, ts=100.0)
    mqtt_sink.publish_live(client, "fbf/s", "b", 2, ts=100.5)
    mqtt_sink.publish_live(client, "fbf/s", "a", 3, ts=100.5)  # COV: the same point twice in one window
    mqtt_sink.flush_batches()
    assert client.published == [("fbf/s/batch", {"points": [
        {"point": "a", "value": 1, "ts": 100.0}, {"point": "b", "value": 2, "ts": 100.5},
        {"point": "a", "value": 3, "ts": 100.5}]}, 1, False)]


def test_the_window_publishes_on_its_own(batch_mode, monkeypatch):
    import time as _time

    monkeypatch.setattr(mqtt_sink, "BATCH_WINDOW_S", 0.05)
    client = FakeClient()
    mqtt_sink.publish_live(client, "fbf/s", "a", 1, ts=1.0)
    deadline = _time.time() + 5
    while not client.published and _time.time() < deadline:
        _time.sleep(0.01)
    assert client.published == [("fbf/s/batch", {"ts": 1.0, "points": {"a": 1}}, 1, False)]


def test_a_full_batch_goes_at_once(batch_mode, monkeypatch):
    monkeypatch.setattr(mqtt_sink, "BATCH_MAX_POINTS", 2)
    client = FakeClient()
    mqtt_sink.publish_live(client, "fbf/s", "a", 1, ts=1.0)
    mqtt_sink.publish_live(client, "fbf/s", "b", 2, ts=1.0)
    assert len(client.published) == 1 and client.published[0][1]["points"] == {"a": 1, "b": 2}


def test_offline_at_publish_time_spools_each_reading_per_point(batch_mode, spool_file):
    client = FakeClient(connected=False)
    mqtt_sink.publish_live(client, "fbf/s", "a", 1, ts=1.0)
    mqtt_sink.publish_live(client, "fbf/s", "b", 2, ts=1.0)
    mqtt_sink.flush_batches()
    assert client.published == []
    assert mqtt_sink._read_spool(spool_file) == [{"topic_prefix": "fbf/s", "point": "a", "value": 1, "ts": 1.0},
                                                 {"topic_prefix": "fbf/s", "point": "b", "value": 2, "ts": 1.0}]


def test_tags_and_history_never_ride_a_batch(batch_mode):
    client = FakeClient()
    mqtt_sink.publish_reading(client, "fbf/s", "a/tags", "{}")
    mqtt_sink.publish_reading(client, "fbf/s", "a", 5, ts=1.0, retain=False)
    assert [p[0] for p in client.published] == ["fbf/s/a/tags", "fbf/s/a"]


def test_a_reading_with_quality_goes_per_point_even_when_batching(batch_mode):
    """Windrow#20 + Baler#234: the batch format carries no quality, so a
    flagged reading is published on its own, quality and all."""
    client = FakeClient()
    q = {"status_flags": ["fault"], "reliability": "no-sensor"}
    assert mqtt_sink.publish_live(client, "fbf/site-a/ahu-1", "sat", 55.0, ts=100.0, quality=q) == "fbf/site-a/ahu-1/sat"
    mqtt_sink.publish_live(client, "fbf/site-a/ahu-1", "fan", "active", ts=100.0)
    mqtt_sink.flush_batches()
    assert client.published == [
        ("fbf/site-a/ahu-1/sat", {"point": "sat", "value": 55.0, "ts": 100.0, "quality": q}, 1, True),
        ("fbf/site-a/ahu-1/batch", {"ts": 100.0, "points": {"fan": "active"}}, 1, False),
    ]


def test_a_reading_with_a_priority_array_goes_per_point_even_when_batching(batch_mode):
    """Windrow#25 + Baler#234: the batch format carries no priority array."""
    client = FakeClient()
    pr = {"active": 8, "levels": {"8": 72.0}}
    assert mqtt_sink.publish_live(client, "fbf/site-a/vav-1", "zsp", 72.0, ts=100.0, priority=pr) == "fbf/site-a/vav-1/zsp"
    assert client.published == [("fbf/site-a/vav-1/zsp", {"point": "zsp", "value": 72.0, "ts": 100.0, "priority": pr}, 1, True)]
