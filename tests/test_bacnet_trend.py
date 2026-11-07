"""Baler#129: trend-log backfill and an outage-sized spool. No device or
broker needed - the live backfill against windrow.mock_device's TrendLog is
in test_bacnet_trend_live.py."""

import asyncio
import datetime as dt
import json
import types

import pytest
from bacpypes3.apdu import ConfirmedServiceChoice, Error, ReadRangeACK
from bacpypes3.basetypes import LogRecord, LogRecordLogDatum, PropertyIdentifier
from bacpypes3.constructeddata import Any, SequenceOf
from bacpypes3.local.device import (
    DeviceObject,  # noqa: F401 - registers standard object classes
)
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.vendor import get_vendor_info

from windrow import bacnet_trend, mqtt_sink
from windrow import connection_manager as cm
from windrow.bacnet_read import MismatchedResponse

# --- timestamps and values ---


def test_bacnet_datetime_round_trip():
    when = dt.datetime(2026, 11, 7, 13, 5, 9, 500000)  # noqa: DTZ001 - BACnet times are naive
    assert bacnet_trend.from_bacnet_datetime(bacnet_trend.to_bacnet_datetime(when)) == when


def test_a_wildcard_timestamp_cant_be_placed():
    from bacpypes3.basetypes import DateTime
    from bacpypes3.primitivedata import Date, Time

    assert bacnet_trend.from_bacnet_datetime(DateTime(date=Date((255, 11, 7, 255)), time=Time((1, 2, 3, 0)))) is None


@pytest.mark.parametrize("datum, object_type, expected", [
    (LogRecordLogDatum(realValue=71.5), "analog-input", 71.5),
    (LogRecordLogDatum(enumValue=1), "binary-value", "active"),
    (LogRecordLogDatum(enumValue=0), "binary-input", "inactive"),
    (LogRecordLogDatum(enumValue=3), "multi-state-value", 3),
    (LogRecordLogDatum(unsignedValue=12), "analog-value", 12),
    (LogRecordLogDatum(booleanValue=True), "binary-value", True),
    (LogRecordLogDatum(timeChange=60.0), "analog-input", None),  # a clock change, not a reading
])
def test_log_datum_values_match_what_a_poll_publishes(datum, object_type, expected):
    assert bacnet_trend.datum_value(datum, object_type) == expected


# --- ReadRange paging ---

EPOCH_LOCAL = dt.datetime(2026, 11, 7, 12, 0, 0)  # noqa: DTZ001 - device clock = UTC in these tests


def _unix(local):
    return local.replace(tzinfo=dt.timezone.utc).timestamp()


class TrendApp:
    """A device whose trend-log,1 holds a record every 10 s from EPOCH_LOCAL,
    served by ReadRange by time like a real one (and like mock_device)."""

    def __init__(self, count=55, answer_for="trend-log,1", raise_error=False):
        self.records = [(EPOCH_LOCAL + dt.timedelta(seconds=10 * i), 60.0 + i) for i in range(count)]
        self.answer_for = answer_for
        self.raise_error = raise_error
        self.requests = 0

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    async def request(self, req):
        self.requests += 1
        if self.raise_error:
            raise Error(service_choice=ConfirmedServiceChoice.readRange, errorClass="object", errorCode="unknownObject")
        after = bacnet_trend.from_bacnet_datetime(req.range.byTime.referenceTime)
        newer = [r for r in self.records if r[0] > after]
        page = newer[: int(req.range.byTime.count)]
        items = [LogRecord(timestamp=bacnet_trend.to_bacnet_datetime(when), logDatum=LogRecordLogDatum(realValue=v),
                           statusFlags=[0, 0, 0, 0]) for when, v in page]
        return ReadRangeACK(objectIdentifier=ObjectIdentifier(self.answer_for), propertyIdentifier=PropertyIdentifier("log-buffer"),
                            resultFlags=[False, len(page) == len(newer), len(newer) > len(page)], itemCount=len(items),
                            itemData=Any(SequenceOf(LogRecord)(items)))


def _backfill(app, since_s, until_s, offset=dt.timedelta(0)):
    since, until = _unix(EPOCH_LOCAL) + since_s, _unix(EPOCH_LOCAL) + until_s
    return asyncio.run(bacnet_trend.backfill(app, "10.0.0.5", "trend-log,1", "analog-value", since, until, offset))


def test_backfill_pages_through_the_gap_only():
    app = TrendApp()
    readings = _backfill(app, 15, 495)  # (15 s, 495 s): records at 20..490 s
    assert [ts - _unix(EPOCH_LOCAL) for ts, _ in readings] == [float(s) for s in range(20, 491, 10)]
    assert readings[0][1] == 62.0
    assert app.requests == 3  # pages of 20 by moreItems; stopped once past the gap


def test_backfill_converts_the_devices_local_clock():
    """A device 5 h behind UTC logs 07:00 for 12:00 UTC."""
    app = TrendApp()
    offset = dt.timedelta(hours=-5)
    since = _unix(EPOCH_LOCAL - offset) + 15  # in UTC: the device's EPOCH_LOCAL + 15 s
    readings = asyncio.run(bacnet_trend.backfill(app, "10.0.0.5", "trend-log,1", "analog-value", since, since + 30, offset))
    assert [ts - since for ts, _ in readings] == [5.0, 15.0, 25.0]


def test_an_answer_for_another_trend_log_is_rejected():
    with pytest.raises(MismatchedResponse):
        _backfill(TrendApp(answer_for="trend-log,9"), 0, 100)


def test_a_raised_error_is_a_trend_read_error_not_a_base_exception():
    with pytest.raises(bacnet_trend.TrendReadError):
        _backfill(TrendApp(raise_error=True), 0, 100)


def test_device_utc_offset_rounds_to_a_quarter_hour(monkeypatch):
    from bacpypes3.primitivedata import Date, Time

    now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    device_now = now_utc - dt.timedelta(hours=5, seconds=40)  # 5 h behind, clock 40 s slow

    async def fake_read(app, address, objid, prop="present-value", timeout=None):
        assert objid == "device,77"
        if prop == "local-date":
            return Date((device_now.year - 1900, device_now.month, device_now.day, device_now.isoweekday()))
        return Time((device_now.hour, device_now.minute, device_now.second, 0))

    monkeypatch.setattr("windrow.bacnet_read.read_present_value", fake_read)
    assert asyncio.run(bacnet_trend.device_utc_offset(None, "10.0.0.5", 77)) == dt.timedelta(hours=-5)


def test_device_utc_offset_is_none_when_the_device_wont_say(monkeypatch):
    async def refuse(app, address, objid, prop="present-value", timeout=None):
        raise Error(service_choice=ConfirmedServiceChoice.readProperty, errorClass="property", errorCode="unknownProperty")

    monkeypatch.setattr("windrow.bacnet_read.read_present_value", refuse)
    assert asyncio.run(bacnet_trend.device_utc_offset(None, "10.0.0.5", 77)) is None


# --- the poller: when it backfills ---


class CapturingMqtt:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload), retain))


@pytest.fixture
def poller(tmp_path, monkeypatch):
    device = {"up": True}
    backfills = []

    async def read(app, address, objid):
        if not device["up"]:
            raise TimeoutError()
        return 70.0

    async def fake_backfill(app, address, trend_log, object_type, since, until, offset, **kw):
        backfills.append((trend_log, since, until))
        return [(since + 1, 68.0), (since + 2, 69.0)]

    async def fake_offset(app, address, instance, timeout=None):
        return dt.timedelta(0)

    monkeypatch.setattr(cm, "read_present_value", read)
    monkeypatch.setattr(cm.bacnet_trend, "backfill", fake_backfill)
    monkeypatch.setattr(cm.bacnet_trend, "device_utc_offset", fake_offset)
    monkeypatch.setattr(cm, "MAX_FAILURE_BACKOFF", 0.05)
    return device, backfills, tmp_path / "connections.json"


RECORD = {"id": "c1", "device_address": "10.0.0.5", "device_instance": 5, "topic_prefix": "fbf/ahu-1", "poll_interval": 0.02,
          "points": [{"object_identifier": "analog-value,1", "label": "zone-temp", "trend_log": "trend-log,1"}]}


async def _wait(cond, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return True
        await asyncio.sleep(0.01)
    return False


def test_restart_backfills_from_the_saved_mark_not_retained(poller, monkeypatch):
    _, backfills, state_file = poller
    mark = __import__("time").time() - 600
    cm._save_marks(str(state_file).replace(".json", ".trend-marks.json"), {"c1/zone-temp": mark})
    mqtt = CapturingMqtt()

    async def run():
        manager = cm.ConnectionManager(None, mqtt, str(state_file))
        manager._marks = cm._load_marks(manager.marks_file)
        task = asyncio.create_task(manager._poll_point(RECORD, RECORD["points"][0]))
        assert await _wait(lambda: backfills)
        await asyncio.sleep(0.05)
        task.cancel()
        return manager

    manager = asyncio.run(run())
    assert len(backfills) == 1 and backfills[0][0] == "trend-log,1" and backfills[0][1] == mark
    history = [(p["ts"], p["value"]) for t, p, retain in mqtt.published if not retain]
    assert history == [(mark + 1, 68.0), (mark + 2, 69.0)]
    assert all(retain for t, p, retain in mqtt.published if p["value"] == 70.0)  # live readings stay retained
    assert manager._marks["c1/zone-temp"] > mark


def test_recovery_after_an_outage_backfills_the_gap(poller):
    device, backfills, state_file = poller
    mqtt = CapturingMqtt()

    async def run():
        manager = cm.ConnectionManager(None, mqtt, str(state_file))
        task = asyncio.create_task(manager._poll_point(RECORD, RECORD["points"][0]))
        assert await _wait(lambda: mqtt.published)
        assert backfills == []  # no mark yet: nothing to backfill on the first read
        device["up"] = False
        down_at = __import__("time").time()
        await asyncio.sleep(1.2)  # a gap under a second isn't worth a ReadRange
        device["up"] = True
        assert await _wait(lambda: backfills)
        task.cancel()
        return down_at

    down_at = asyncio.run(run())
    _, since, until = backfills[0]
    assert since <= down_at < until  # from the last good read before the outage to the first after


def test_backfill_window_caps_how_far_back_and_zero_turns_it_off(poller):
    _, backfills, state_file = poller
    mqtt = CapturingMqtt()

    async def run():
        manager = cm.ConnectionManager(None, mqtt, str(state_file))
        until = __import__("time").time()
        assert await manager._backfill({**RECORD, "backfill_window": 60}, RECORD["points"][0], until - 3600, until) == 2
        assert backfills[-1][1] == pytest.approx(until - 60)
        assert await manager._backfill({**RECORD, "backfill_window": 0}, RECORD["points"][0], until - 3600, until) == 0
        assert len(backfills) == 1

    asyncio.run(run())


def test_a_failed_backfill_doesnt_stop_polling(poller, monkeypatch):
    _, _, state_file = poller

    async def broken(*a, **kw):
        raise bacnet_trend.TrendReadError("no such trend log")

    monkeypatch.setattr(cm.bacnet_trend, "backfill", broken)
    mqtt = CapturingMqtt()

    async def run():
        manager = cm.ConnectionManager(None, mqtt, str(state_file))
        manager._marks = {"c1/zone-temp": __import__("time").time() - 60}
        task = asyncio.create_task(manager._poll_point(RECORD, RECORD["points"][0]))
        await asyncio.sleep(0.2)
        assert not task.done()
        task.cancel()

    asyncio.run(run())
    assert sum(1 for _, p, _ in mqtt.published if p["value"] == 70.0) >= 3


def test_trend_log_and_backfill_window_are_validated():
    assert cm.validate_points([{"object_identifier": "analog-value,1", "label": "a", "trend_log": "trend-log,1"}])[0][
        "trend_log"] == "trend-log,1"
    with pytest.raises(ValueError):
        cm.validate_points([{"object_identifier": "analog-value,1", "label": "a", "trend_log": ""}])
    with pytest.raises(ValueError):
        cm.check_backfill_window(-1)
    assert cm.check_backfill_window(0) == 0


def test_reading_rate_sums_every_points_poll_rate():
    manager = cm.ConnectionManager(None, None, "/nonexistent/c.json")
    manager.connections = {"a": {"poll_interval": 5.0, "points": [{"label": "x"}, {"label": "y", "poll_interval": 1.0}]}}
    assert manager.reading_rate() == pytest.approx(1.2)


# --- the spool, sized by outage ---


@pytest.fixture
def spool(tmp_path, monkeypatch):
    path = tmp_path / "spool.jsonl"
    monkeypatch.setenv("WINDROW_SPOOL_FILE", str(path))
    monkeypatch.setattr(mqtt_sink, "_spool_rows", {})
    monkeypatch.setattr(mqtt_sink, "_reading_rate", None)
    return path


def test_spool_capacity_covers_the_outage_at_the_reading_rate(spool, monkeypatch):
    monkeypatch.setattr(mqtt_sink, "SPOOL_OUTAGE_HOURS", 24.0)
    mqtt_sink.set_reading_rate(lambda: 100.0)  # 500 points on a 5 s poll
    assert mqtt_sink.spool_capacity() == 5_000_000  # 9.5M wanted: the disk ceiling wins
    monkeypatch.setattr(mqtt_sink, "SPOOL_OUTAGE_HOURS", 2.0)
    assert mqtt_sink.spool_capacity() == int(100 * 7200 * 1.1)
    mqtt_sink.set_reading_rate(lambda: 0.001)
    assert mqtt_sink.spool_capacity() == mqtt_sink.MIN_SPOOL_ROWS


def test_a_full_spool_logs_what_it_dropped(spool, monkeypatch, capsys):
    monkeypatch.setattr(mqtt_sink, "MAX_SPOOL_ROWS", 3)
    client = types.SimpleNamespace(is_connected=lambda: False)
    for i in range(5):
        mqtt_sink.publish_reading(client, "fbf/a", "p", i, ts=float(i))
    assert [r["ts"] for r in mqtt_sink._read_spool(str(spool))] == [2.0, 3.0, 4.0]
    out = capsys.readouterr().out
    assert "dropped the 1 oldest reading(s) of 1 point(s), ts 1.0 to 1.0" in out
    assert mqtt_sink.spool_stats()["dropped"] >= 2


def test_spooled_history_replays_not_retained(spool):
    published = []
    client = types.SimpleNamespace(is_connected=lambda: False)
    mqtt_sink.publish_reading(client, "fbf/a", "p", 68.0, ts=1.0, retain=False)
    mqtt_sink.publish_reading(client, "fbf/a", "p", 70.0, ts=2.0)
    online = types.SimpleNamespace(is_connected=lambda: True,
                                   publish=lambda topic, payload, qos=0, retain=False: published.append((json.loads(payload)["ts"], retain)))
    mqtt_sink._flush_spool(online)
    assert published == [(1.0, False), (2.0, True)]
    assert mqtt_sink.spool_stats()["rows"] == 0
