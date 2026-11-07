"""Baler#84: a BACnet device reboot used to silence its connection until
windrow_api restarted - bacpypes3's request never returns once the device
stops answering. Reads are now bounded, a failed read publishes nothing,
and polling resumes on its own. No broker or device needed."""

import asyncio
import types

import pytest

from windrow import bacnet_read
from windrow import connection_manager as cm


class HangingApp:
    """Answers the parse calls like bacpypes3 and never answers the request."""

    async def get_vendor_info(self, device_address=None):
        return None

    async def parse_object_identifier(self, objid, vendor_info=None):
        return tuple(objid.split(","))

    async def parse_property_reference(self, prop, vendor_info=None):
        return types.SimpleNamespace(propertyIdentifier=prop)

    def request(self, apdu):
        return asyncio.get_event_loop().create_future()  # never resolved


def test_a_read_that_never_returns_times_out():
    async def run():
        with pytest.raises(TimeoutError):
            await bacnet_read.read_present_value(HangingApp(), "10.0.0.5", "analog-value,1", timeout=0.05)

    asyncio.run(run())


def test_poll_publishes_nothing_while_down_and_resumes(tmp_path, monkeypatch, capsys):
    device = {"up": True, "value": 68.0}
    published = []

    async def fake_read(app, address, objid):
        if not device["up"]:
            raise TimeoutError()
        return device["value"]

    monkeypatch.setattr(cm, "read_present_value", fake_read)
    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append(value))
    mgr = cm.ConnectionManager(app=None, mqtt_client=None, state_file=str(tmp_path / "c.json"))
    record = {"device_address": "10.0.0.5", "poll_interval": 0.01, "topic_prefix": "fbf/t"}
    point = {"object_identifier": "analog-value,1", "label": "zone-temp"}

    async def wait_for(cond, timeout=3.0):
        for _ in range(int(timeout / 0.01)):
            if cond():
                return True
            await asyncio.sleep(0.01)
        return False

    async def run():
        task = asyncio.create_task(mgr._poll_point(record, point))
        try:
            assert await wait_for(lambda: 68.0 in published)
            device["up"] = False
            await asyncio.sleep(0.05)
            published.clear()
            await asyncio.sleep(0.1)
            assert published == []  # no null readings while the device is down
            device.update(up=True, value=69.0)
            assert await wait_for(lambda: 69.0 in published)
        finally:
            task.cancel()

    asyncio.run(run())
    out = capsys.readouterr().out
    assert "read failed for analog-value,1 (1x)" in out and "read recovered for analog-value,1" in out


def test_an_error_answer_is_a_failed_read_not_a_value(tmp_path, monkeypatch):
    from bacpypes3.apdu import Error

    published = []

    async def error_read(app, address, objid):
        return Error(errorClass="object", errorCode="unknownObject")

    monkeypatch.setattr(cm, "read_present_value", error_read)
    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append(value))
    mgr = cm.ConnectionManager(app=None, mqtt_client=None, state_file=str(tmp_path / "c.json"))

    async def run():
        task = asyncio.create_task(mgr._poll_point({"device_address": "x", "poll_interval": 0.01, "topic_prefix": "fbf/t"},
                                                   {"object_identifier": "analog-value,9", "label": "p"}))
        await asyncio.sleep(0.1)
        task.cancel()

    asyncio.run(run())
    assert published == []
