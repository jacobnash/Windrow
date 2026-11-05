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
