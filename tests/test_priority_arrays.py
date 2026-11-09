"""Windrow#25: commandable points' priority arrays read on an interval and
published with the point's reading as `priority`, so Baler can see a level
held by someone else (a human override at the panel) - Baler#212's passive
override tests. Fakes for the logic; the real mock device over loopback for
the encoding. Nothing here writes to anything but the mock device."""

import asyncio
import contextlib
import json
import random
import socket

import pytest
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import ErrorType, PriorityValue
from bacpypes3.local.cmd import PriorityArray
from bacpypes3.primitivedata import Null

from windrow import connection_manager as cm
from windrow import mqtt_sink
from windrow.bacnet_read import RPMUnsupported
from windrow.bacnet_values import priority_levels


def _array(**held):
    """PriorityArray with e.g. p8=72.0 held; everything else null."""
    slots = [PriorityValue(null=()) for _ in range(16)]
    for key, value in held.items():
        level = int(key[1:])
        slots[level - 1] = PriorityValue(enumerated=value) if isinstance(value, str) else PriorityValue(real=value)
    return PriorityArray(slots)


def test_priority_levels_lists_held_levels_and_which_is_in_control():
    assert priority_levels(_array(p8=72.0, p16=68.0)) == {"active": 8, "levels": {"8": 72.0, "16": 68.0}}
    assert priority_levels(_array()) == {"active": None, "levels": {}}  # relinquish-default in control
    assert priority_levels([1, 2, 3]) is None
    assert priority_levels(None) is None


# --- the poll loop ---


class NullMqtt:
    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        pass


def _run(tmp_path, monkeypatch, arrays, seconds=0.5, record_extra=None, rpm="ok", points=None):
    """Poll a connection whose device answers present-value 70.0 and the
    priority arrays from `arrays` in turn (the last repeats). Returns the
    published (label, value, priority) and the priority requests made."""
    answers = list(arrays)
    requests = []

    async def read_many(app, address, objids, prop="present-value", timeout=None):
        if prop == "priority-array":
            requests.append(("rpm", list(objids)))
            if rpm == "reject":
                raise RPMUnsupported("no rpm")
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
            return {o: answer(o) if callable(answer) else answer for o in objids}
        return {o: 70.0 for o in objids}

    async def read_one(app, address, objid, prop="present-value", timeout=None):
        if prop == "priority-array":
            requests.append(("single", objid))
            answer = answers.pop(0) if len(answers) > 1 else answers[0]
            return answer(objid) if callable(answer) else answer
        return 70.0

    monkeypatch.setattr(cm, "read_present_values", read_many)
    monkeypatch.setattr(cm, "read_present_value", read_one)
    monkeypatch.setattr(cm.random, "uniform", lambda a, b: 0.0)
    published = []

    def publish(client, prefix, label, value, priority=None, **_):
        published.append((label, value, priority))

    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", publish)

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": "10.0.0.5", "topic_prefix": "fbf/t", "poll_interval": 0.05,
                  "priority_interval": 0.1,
                  "points": points or [{"object_identifier": "analog-value,1", "label": "sp", "his_collect_cov": True},
                                       {"object_identifier": "analog-input,2", "label": "sat"}],
                  **(record_extra or {})}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(seconds)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return manager

    manager = asyncio.run(run())
    return published, requests, manager


def test_a_commandable_points_reading_carries_its_priority_array(tmp_path, monkeypatch):
    published, requests, _ = _run(tmp_path, monkeypatch, [_array(p8=72.0, p16=70.0)])
    sp = [p for label, _, p in published if label == "sp"]
    assert {"active": 8, "levels": {"8": 72.0, "16": 70.0}} in sp
    # only commandable points are asked, and the sensor's reading has no priority
    assert all(objids == ["analog-value,1"] for kind, objids in requests if kind == "rpm")
    assert all(p is None for label, _, p in published if label == "sat")


def test_an_override_at_the_same_value_is_published_despite_cov_policy(tmp_path, monkeypatch):
    # sp is his_collect_cov: an unchanged 70.0 isn't republished - unless
    # someone takes (or releases) a level, which is exactly the news
    published, _, _ = _run(tmp_path, monkeypatch,
                           [_array(p16=70.0), _array(p16=70.0), _array(p8=70.0, p16=70.0), _array(p8=70.0, p16=70.0),
                            _array(p16=70.0)], seconds=0.8)
    sp = [p for label, _, p in published if label == "sp"]
    actives = [p["active"] if p else None for p in sp]
    assert 8 in actives and actives.index(8) > 0
    assert 16 in actives[actives.index(8):]  # and the release is published too
    assert len(sp) <= 6  # not every poll: only when the array changed


def test_a_device_without_rpm_is_asked_one_point_at_a_time(tmp_path, monkeypatch):
    _, requests, _ = _run(tmp_path, monkeypatch, [_array(p16=1.0)], rpm="reject", points=[
        {"object_identifier": "analog-value,1", "label": "a"}, {"object_identifier": "analog-value,2", "label": "b"}])
    kinds = [kind for kind, _ in requests]
    assert kinds[0] == "rpm" and kinds.count("rpm") == 1  # rejected once, never asked again
    assert "single" in kinds


def test_an_object_without_a_priority_array_isnt_asked_again(tmp_path, monkeypatch):
    unknown = ErrorType(errorClass="property", errorCode="unknownProperty")
    _, requests, _ = _run(tmp_path, monkeypatch, [lambda o: unknown if o.endswith(",1") else _array(p16=1.0)], points=[
        {"object_identifier": "analog-value,1", "label": "a"}, {"object_identifier": "analog-value,2", "label": "b"}])
    rpm_asks = [objids for kind, objids in requests if kind == "rpm"]
    assert rpm_asks[0] == ["analog-value,1", "analog-value,2"]
    assert all("analog-value,1" not in a for a in rpm_asks[1:])


def test_priority_interval_zero_turns_it_off(tmp_path, monkeypatch):
    published, requests, _ = _run(tmp_path, monkeypatch, [_array(p8=1.0)], record_extra={"priority_interval": 0})
    assert requests == [] and all(p is None for _, _, p in published)


def test_priority_interval_is_validated():
    assert cm.validate_read_options({"priority_interval": 0}) == {"priority_interval": 0}
    assert cm.validate_read_options({"priority_interval": 120}) == {"priority_interval": 120}
    for bad in (-1, 1, "60", True):
        with pytest.raises(ValueError, match="priority_interval"):
            cm.validate_read_options({"priority_interval": bad})


# --- the envelope ---


class FakeClient:
    def __init__(self, connected=True):
        self._connected = connected
        self.published = []

    def is_connected(self):
        return self._connected

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append(json.loads(payload))


def test_the_envelope_carries_priority_only_when_given_and_through_the_spool(tmp_path, monkeypatch):
    monkeypatch.setenv("WINDROW_SPOOL_FILE", str(tmp_path / "spool.jsonl"))
    held = {"active": 8, "levels": {"8": 72.0}}
    client = FakeClient(connected=False)
    mqtt_sink.publish_reading(client, "fbf/a", "sp", 72.0, ts=1.0, priority=held)
    mqtt_sink.publish_reading(client, "fbf/a", "sat", 55.0, ts=1.0)
    client._connected = True
    mqtt_sink._flush_spool(client)
    assert client.published == [{"point": "sp", "value": 72.0, "ts": 1.0, "priority": held},
                                {"point": "sat", "value": 55.0, "ts": 1.0}]


# --- the real mock device, over loopback ---


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


def test_an_override_on_the_mock_device_shows_up_in_the_published_priority(tmp_path):
    """A "panel" write at priority 8 (to the mock, by a second client -
    Windrow itself writes nothing here) must show up as active 8, and its
    release as nothing held again."""
    from bacpypes3.app import Application

    from windrow.mock_device import MockDevice

    device_port, windrow_port, panel_port = _free_udp_port(), _free_udp_port(), _free_udp_port()
    published = []

    class Mqtt(NullMqtt):
        def publish(self, topic, payload, qos=0, retain=False):
            published.append((topic, json.loads(payload)))

    def parse(port, instance, name=None):
        argv = ["--address", f"127.0.0.1/32:{port}", "--instance", str(instance)] + (["--name", name] if name else [])
        return SimpleArgumentParser().parse_args(argv)

    async def run():
        mock = MockDevice(parse(device_port, 3456, "mock"))
        windrow_app = Application.from_args(parse(windrow_port, 4194002))
        panel = Application.from_args(parse(panel_port, 4194003))
        address = f"127.0.0.1:{device_port}"
        manager = cm.ConnectionManager(windrow_app, Mqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": address, "device_instance": 3456, "topic_prefix": "fbf/mock",
                  "poll_interval": 0.2, "priority_interval": 0.3,
                  "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}]}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        try:
            await asyncio.sleep(1.0)
            await panel.write_property(address, "analog-value,1", "present-value", 75.0, priority=8)
            await asyncio.sleep(1.0)
            await panel.write_property(address, "analog-value,1", "present-value", Null(()), priority=8)  # relinquish
            await asyncio.sleep(1.0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            windrow_app.close()
            panel.close()
            mock.app.close()

    asyncio.run(run())
    priorities = [p["priority"] for t, p in published if t == "fbf/mock/zone-temp" and "priority" in p]
    actives = [p["active"] for p in priorities]
    # bacpypes3's simulated presentValue sets no level: relinquish-default is in control
    assert actives[0] is None and 8 in actives, priorities
    overridden = next(p for p in priorities if p["active"] == 8)
    assert overridden["levels"] == {"8": 75.0}
    assert priorities[-1] == {"active": None, "levels": {}}  # released
