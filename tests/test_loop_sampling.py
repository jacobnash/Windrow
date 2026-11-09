"""Windrow#27: a fast/COV sampling profile for control-loop points, so a
4-minute hunt isn't invisible behind a 15-minute poll. Fakes for the
logic, and the real mock device over loopback for the COV path (the only
write is to the mock device, by a test client standing in for a panel)."""

import asyncio
import contextlib
import json
import random
import socket

import pytest
from bacpypes3.apdu import SimpleAckPDU
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.local.device import (
    DeviceObject,  # noqa: F401 - registers standard object classes
)
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.vendor import get_vendor_info

from windrow import connection_manager as cm


def test_profile_precedence():
    rec = {"sampling": "auto"}
    assert cm.sampling_profile(rec, {"object_identifier": "analog-output,1"}) == "loop"
    assert cm.sampling_profile(rec, {"object_identifier": "binary-output,2"}) == "loop"
    assert cm.sampling_profile(rec, {"object_identifier": "analog-input,3"}) == "standard"
    assert cm.sampling_profile(rec, {"object_identifier": "analog-input,3", "sampling": "loop"}) == "loop"
    assert cm.sampling_profile({"sampling": "loop"}, {"object_identifier": "analog-input,1", "sampling": "standard"}) == "standard"
    assert cm.sampling_profile({}, {"object_identifier": "analog-output,1"}) == "standard"  # opt-in


def test_sampling_is_validated():
    assert cm.validate_read_options({"sampling": "auto"}) == {"sampling": "auto"}
    with pytest.raises(ValueError, match="sampling"):
        cm.validate_read_options({"sampling": "fast"})
    (point,) = cm.validate_points([{"object_identifier": "analog-input,1", "label": "dat", "sampling": "loop"}])
    assert point["sampling"] == "loop"
    with pytest.raises(ValueError, match="sampling"):
        cm.validate_points([{"object_identifier": "analog-input,1", "label": "dat", "sampling": "auto"}])


# --- the poll loop ---


class CovApp:
    def __init__(self, answer="ack"):
        self.answer = answer
        self._cov_contexts = {}
        self.subscribed = []

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    def request(self, req):
        future = asyncio.get_event_loop().create_future()
        if req.lifetime is not None:
            self.subscribed.append(str(req.monitoredObjectIdentifier))
        if self.answer == "ack" or req.lifetime is None:
            future.set_result(SimpleAckPDU())
        else:
            from bacpypes3.apdu import ConfirmedServiceChoice, Error

            future.set_result(Error(service_choice=ConfirmedServiceChoice.subscribeCOV, errorClass="object",
                                    errorCode="optionalFunctionalityNotSupported"))
        return future


class NullMqtt:
    def is_connected(self):
        return True

    def publish(self, *a, **k):
        pass


def _run(tmp_path, monkeypatch, app, points, seconds=0.6, record_extra=None):
    polls = {}

    async def read_one(app_, address, objid):
        polls[objid] = polls.get(objid, 0) + 1
        return 1.0

    monkeypatch.setattr(cm, "read_present_value", read_one)
    monkeypatch.setattr(cm, "LOOP_POLL_INTERVAL", 0.05)
    monkeypatch.setattr(cm.random, "uniform", lambda a, b: 0.0)

    async def run():
        manager = cm.ConnectionManager(app, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": "10.0.0.5", "topic_prefix": "fbf/t", "poll_interval": 900.0,
                  "read_multiple": False, "points": points, **(record_extra or {})}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(seconds)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

    asyncio.run(run())
    return polls


def test_a_loop_point_without_cov_is_polled_fast_and_a_standard_one_isnt(tmp_path, monkeypatch):
    app = CovApp("unsupported")
    polls = _run(tmp_path, monkeypatch, app, [
        {"object_identifier": "analog-output,1", "label": "vlv", "sampling": "loop"},
        {"object_identifier": "analog-input,2", "label": "oat"},
    ])
    assert app.subscribed == ["analog-output,1"]  # loop points try COV first; standard ones don't
    assert polls.get("analog-output,1", 0) >= 4  # COV refused: polled at the loop rate, not every 900 s
    assert polls.get("analog-input,2", 0) <= 1


def test_auto_subscribes_outputs_and_a_point_can_still_opt_out(tmp_path, monkeypatch):
    app = CovApp("ack")
    _run(tmp_path, monkeypatch, app, [
        {"object_identifier": "analog-output,1", "label": "vlv"},
        {"object_identifier": "binary-output,2", "label": "fan-cmd", "subscribe_cov": False},
        {"object_identifier": "analog-input,3", "label": "dat"},
    ], seconds=0.3, record_extra={"sampling": "auto"})
    assert app.subscribed == ["analog-output,1"]


# --- the real mock device, over loopback: a change arrives by COV, not by the 15-minute poll ---


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


def test_a_loop_point_on_the_mock_device_publishes_changes_by_cov(tmp_path, monkeypatch):
    from bacpypes3.app import Application

    from windrow.mock_device import MockDevice

    monkeypatch.setattr(cm.random, "uniform", lambda a, b: 0.0)
    device_port, windrow_port, panel_port = _free_udp_port(), _free_udp_port(), _free_udp_port()
    published = []

    class Mqtt(NullMqtt):
        def publish(self, topic, payload, qos=0, retain=False):
            published.append((asyncio.get_event_loop().time(), topic, json.loads(payload)["value"]))

    def parse(port, instance, name=None):
        argv = ["--address", f"127.0.0.1/32:{port}", "--instance", str(instance)] + (["--name", name] if name else [])
        return SimpleArgumentParser().parse_args(argv)

    async def run():
        mock = MockDevice(parse(device_port, 3456, "mock"))
        windrow_app = Application.from_args(parse(windrow_port, 4194004))
        panel = Application.from_args(parse(panel_port, 4194005))
        address = f"127.0.0.1:{device_port}"
        manager = cm.ConnectionManager(windrow_app, Mqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": address, "device_instance": 3456, "topic_prefix": "fbf/mock",
                  "poll_interval": 900.0, "points": [{"object_identifier": "analog-value,1", "label": "zone-temp",
                                                      "sampling": "loop"}]}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        try:
            for _ in range(40):  # subscribed?
                await asyncio.sleep(0.1)
                if manager.read_stats()["cov_subscriptions"]:
                    break
            stats_before = dict(manager.read_stats())
            written_at = asyncio.get_event_loop().time()
            await panel.write_property(address, "analog-value,1", "present-value", 81.5, priority=8)
            await asyncio.sleep(1.0)
            return stats_before, written_at, dict(manager.read_stats())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            windrow_app.close()
            panel.close()
            mock.app.close()

    before, written_at, after = asyncio.run(run())
    assert before["cov_subscriptions"] == 1
    arrived = [t for t, topic, value in published if topic == "fbf/mock/zone-temp" and value == 81.5]
    assert arrived and arrived[0] - written_at < 1.0
    assert after.get("cov_notifications", 0) >= 1
