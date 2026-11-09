"""Windrow#26: write BACnet Schedule and Calendar objects through the
command channel, in bacnet_schedules' read format, with point_write's
safeguards (expires_at, validate before sending, bounded requests, a
device's refusal reported). Every write here goes to windrow.mock_device
over loopback, or to nothing at all - never a real device."""

import asyncio
import contextlib
import json
import random
import socket
import threading
import time
from types import SimpleNamespace

import pytest
from bacpypes3.argparse import SimpleArgumentParser

from windrow import bacnet_schedule_write as bsw
from windrow import connection_manager as cm
from windrow.api import _make_on_command, command_body

WEEK = {d: [{"time": "06:30", "value": "active"}, {"time": "20:00", "value": "inactive"}]
        for d in ("monday", "tuesday", "wednesday", "thursday", "friday")} | {"saturday": [], "sunday": []}
BINARY = {"controls": [{"object": "binary-value,1", "property": "present-value"}], "default": "inactive"}


# --- validation: everything is checked before anything is sent ---


def test_a_full_schedule_write_validates_into_writes_in_order():
    writes = bsw.validate_schedule({
        "weekly": WEEK, "default": "inactive", "effective_period": ["2026-01-01", "2026-12-31"],
        "exceptions": [{"period": {"calendar": "calendar,1"}, "priority": 1, "times": [{"time": "00:00", "value": "inactive"}]},
                       {"period": {"week_n_day": {"month": 11, "week": 4, "weekday": "thursday"}}, "priority": 10,
                        "times": [{"time": "07:00", "value": "active"}]},
                       {"period": {"date_range": ["2026-12-24", "2026-12-26"]}, "times": []},
                       {"period": {"date": "*-07-04"}, "priority": 5, "times": [{"time": "00:00", "value": None}]}],
    }, BINARY)
    assert [p for p, _ in writes] == ["effective-period", "weekly-schedule", "exception-schedule", "schedule-default"]


@pytest.mark.parametrize("body,match", [
    ({"weekly": {"monday": []}}, "all seven days"),
    ({"weekly": WEEK | {"monday": [{"time": "25:00", "value": "active"}]}}, "out of range"),
    ({"weekly": WEEK | {"monday": [{"time": "7am", "value": "active"}]}}, "HH:MM"),
    ({"weekly": WEEK | {"monday": [{"time": "07:00", "value": 72.5}]}}, "active/inactive"),
    ({"exceptions": [{"period": {"date": "2026-13-45"}, "times": []}]}, "out of range"),
    ({"exceptions": [{"period": {"calendar": "schedule,1"}, "times": []}]}, "calendar object id"),
    ({"exceptions": [{"period": {"date": "2026-12-25"}, "priority": 0, "times": []}]}, "priority is 1-16"),
    ({"default": "inactive", "colour": "red"}, "unknown schedule field"),
    ({}, "nothing to write"),
])
def test_a_malformed_schedule_write_is_refused(body, match):
    with pytest.raises(bsw.ScheduleWriteError, match=match):
        bsw.validate_schedule(body, BINARY)


def test_values_are_encoded_for_what_the_schedule_drives():
    from bacpypes3.primitivedata import Enumerated, Null, Real, Unsigned

    assert isinstance(bsw.encode_value("active", "binary", "v"), Enumerated)
    assert isinstance(bsw.encode_value(72.5, "analog", "v"), Real)
    assert isinstance(bsw.encode_value(3, "multistate", "v"), Unsigned)
    assert isinstance(bsw.encode_value(None, "analog", "v"), Null)  # relinquish
    assert bsw.schedule_value_kind({"controls": [{"object": "analog-value,3"}]}) == "analog"
    assert bsw.schedule_value_kind({"controls": [{"object": "multi-state-value,2"}]}) == "multistate"
    with pytest.raises(bsw.ScheduleWriteError):
        bsw.encode_value(0, "multistate", "v")


def test_a_calendar_takes_dates_but_no_calendar_references():
    assert [p for p, _ in bsw.validate_calendar({"dates": [{"date": "2026-12-25"}, {"date_range": ["2026-12-24", "2026-12-31"]},
                                                          {"week_n_day": {"month": None, "week": None, "weekday": "sunday"}}]})] == ["date-list"]
    with pytest.raises(bsw.ScheduleWriteError):
        bsw.validate_calendar({"dates": [{"calendar": "calendar,2"}]})
    with pytest.raises(bsw.ScheduleWriteError):
        bsw.validate_calendar({"dates": "2026-12-25"})


# --- the command channel ---


class FakeManager:
    def __init__(self):
        self.calls = []

    async def write_schedule(self, connection_id, body, expires_at=None, who=None, reason=None):
        self.calls.append(("write_schedule", connection_id, body, expires_at, who, reason))
        return {"object": body["object"]}

    async def write_calendar(self, connection_id, body, expires_at=None, who=None, reason=None):
        self.calls.append(("write_calendar", connection_id, body, expires_at, who, reason))


@pytest.fixture
def loop_thread():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=2)


class Client:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload)))


def test_schedule_writes_arrive_over_the_command_channel_with_who_why_and_expiry(loop_thread):
    manager = FakeManager()
    on_command = _make_on_command(manager, loop_thread, "site-a", "inst-1")
    client = Client()
    expires = time.time() + 30
    for payload in (
        {"action": "write_schedule", "id": "c1", "request_id": "r1", "expires_at": expires, "object": "schedule,1",
         "default": "inactive", "who": "op:abc", "reason": "holiday hours"},
        {"action": "write_calendar", "id": "nope", "request_id": "r2", "object": "calendar,1", "dates": []},
    ):
        on_command(client, None, SimpleNamespace(topic="cmd/site-a/inst-1/write-schedule", payload=json.dumps(payload).encode()))
    assert manager.calls[0] == ("write_schedule", "c1", {"object": "schedule,1", "default": "inactive"}, expires,
                                "op:abc", "holiday hours")
    (t1, ok), (_t2, missing) = client.published
    assert t1 == "cmd/site-a/inst-1/result" and ok["ok"] is True and ok["request_id"] == "r1"
    assert missing == {"ok": False, "action": "write_calendar", "id": "nope", "request_id": "r2",
                       "error": "no connection with that id"}
    # expires_at reaches the write (it's a parameter there), as for point_write
    assert command_body("write_schedule", {"action": "write_schedule", "id": "c1", "expires_at": 5})["expires_at"] == 5


def test_an_expired_write_is_refused_before_anything_is_read_or_sent(tmp_path):
    manager = cm.ConnectionManager(None, None, str(tmp_path / "c.json"))  # no app: anything sent would blow up
    manager.connections["c1"] = {"id": "c1", "device_address": "10.0.0.5", "device_instance": 1, "points": []}
    with pytest.raises(cm.PointWriteError, match="expired"):
        asyncio.run(manager.write_schedule("c1", {"object": "schedule,1", "default": 1}, expires_at=time.time() - 1))
    with pytest.raises(cm.PointWriteError, match="object is a schedule"):
        asyncio.run(manager.write_schedule("c1", {"object": "calendar,1", "default": 1}))
    assert asyncio.run(manager.write_schedule("missing", {"object": "schedule,1"})) is None


# --- the mock device over loopback: write, read back ---


def _free_udp_port() -> int:
    for _ in range(20):
        port = random.randint(40000, 60000)
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    pytest.skip("no free UDP port on loopback")


def _with_mock(tmp_path, body):
    from bacpypes3.app import Application

    from windrow.mock_device import MockDevice

    device_port, windrow_port = _free_udp_port(), _free_udp_port()

    def parse(port, instance):
        return SimpleArgumentParser().parse_args(["--address", f"127.0.0.1/32:{port}", "--instance", str(instance)])

    async def run():
        mock = MockDevice(parse(device_port, 3456), schedules=True)
        app = Application.from_args(parse(windrow_port, 4194006))
        manager = cm.ConnectionManager(app, None, str(tmp_path / "c.json"))
        manager.connections["c1"] = {"id": "c1", "device_address": f"127.0.0.1:{device_port}", "device_instance": 3456,
                                     "topic_prefix": "fbf/mock",
                                     "points": [{"object_identifier": "binary-value,1", "label": "fan-status"}]}
        try:
            await asyncio.sleep(0.5)
            return await body(manager)
        finally:
            app.close()
            mock.app.close()

    return asyncio.run(run())


def test_write_a_schedule_on_the_mock_device_and_read_it_back(tmp_path):
    exceptions = [{"period": {"date": "2026-12-24"}, "priority": 3,
                   "times": [{"time": "08:00", "value": "active"}, {"time": "12:00", "value": "inactive"}]}]

    async def body(manager):
        before = (await manager.connection_schedules("c1"))["schedules"][0]
        after = await manager.write_schedule("c1", {"object": "schedule,1", "weekly": WEEK, "exceptions": exceptions,
                                                    "effective_period": ["2026-01-01", "2026-12-31"]},
                                             expires_at=time.time() + 30, who="op:t1", reason="test")
        reread = (await manager.connection_schedules("c1"))["schedules"][0]
        # the old schedule written back, as Baler's revert does
        reverted = await manager.write_schedule("c1", {"object": "schedule,1", "weekly": before["weekly"],
                                                       "exceptions": before["exceptions"],
                                                       "effective_period": before["effective_period"]})
        return before, after, reread, reverted

    before, after, reread, reverted = _with_mock(tmp_path, body)
    assert before["weekly"]["monday"][0]["time"] == "07:00:00"
    assert after["weekly"]["monday"] == [{"time": "06:30:00", "value": "active"}, {"time": "20:00:00", "value": "inactive"}]
    assert after["weekly"]["saturday"] == []
    assert after["exceptions"] == [{"period": {"date": "2026-12-24"}, "priority": 3,
                                    "times": [{"time": "08:00:00", "value": "active"}, {"time": "12:00:00", "value": "inactive"}]}]
    assert after["effective_period"] == ["2026-01-01", "2026-12-31"]
    assert after["controls"][0]["label"] == "fan-status"  # the read format, labels and all
    assert reread == after
    assert reverted["weekly"] == before["weekly"] and reverted["exceptions"] == before["exceptions"]


def test_write_a_calendar_on_the_mock_device(tmp_path):
    dates = [{"date": "2026-12-25"}, {"date_range": ["2026-12-28", "2026-12-31"]},
             {"week_n_day": {"month": 11, "week": 4, "weekday": "thursday"}}]

    async def body(manager):
        return await manager.write_calendar("c1", {"object": "calendar,1", "dates": dates})

    after = _with_mock(tmp_path, body)
    assert after["object"] == "calendar,1" and after["dates"] == dates


def test_a_bad_write_to_the_mock_sends_nothing_and_a_missing_object_is_refused(tmp_path):
    async def body(manager):
        before = (await manager.connection_schedules("c1"))["schedules"][0]
        with pytest.raises(cm.PointWriteError, match="active/inactive"):
            await manager.write_schedule("c1", {"object": "schedule,1", "weekly": WEEK, "default": 72.5})
        with pytest.raises(cm.PointWriteError, match="can't read schedule,9"):
            await manager.write_schedule("c1", {"object": "schedule,9", "default": "inactive"})
        return before, (await manager.connection_schedules("c1"))["schedules"][0]

    before, after = _with_mock(tmp_path, body)
    assert after == before  # the valid weekly in the refused command wasn't written either
