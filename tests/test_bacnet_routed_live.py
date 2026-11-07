"""Baler#126's acceptance, live: a mock device behind a simulated router/BBMD
on another network is discovered and polled.

windrow.mock_router runs a BACnet/IP router + BBMD on loopback, with mock
devices on a separate (virtual, MS/TP-like) network 2 behind it. Windrow's
Application binds a different UDP port, so its own broadcasts never reach
the router - as if it were on another subnet - and it only sees the router
by registering with the BBMD as a foreign device. Real BACnet/IP packets
over loopback UDP; no broker (readings are captured)."""

import asyncio
import json
import socket
import subprocess
import sys
import time

import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import bacnet_network, discovery
from windrow.connection_manager import ConnectionManager


class CapturingMqtt:
    def __init__(self):
        self.readings = {}  # topic -> last value

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        if topic.endswith("/tags"):
            return
        body = json.loads(payload) if payload else None
        if isinstance(body, dict) and "value" in body:
            self.readings[topic] = body["value"]


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def router():
    port = _free_udp_port()
    proc = subprocess.Popen([sys.executable, "-m", "windrow.mock_router", "--address", f"127.0.0.1/8:{port}", "--bbmd",
                             "--device", "5:4242", "--device", "6:4243"])
    try:
        time.sleep(2)
        yield f"127.0.0.1:{port}"
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _app(monkeypatch, bbmd: str | None) -> Application:
    """Built the way windrow.api builds its Application: SimpleArgumentParser
    plus the WINDROW_BACNET_* env defaults."""
    monkeypatch.delenv("WINDROW_BACNET_FOREIGN_BBMD", raising=False)
    if bbmd:
        monkeypatch.setenv("WINDROW_BACNET_FOREIGN_BBMD", bbmd)
        monkeypatch.setenv("WINDROW_BACNET_FOREIGN_TTL", "30")
    parser = SimpleArgumentParser()
    bacnet_network.add_network_args(parser)
    args = parser.parse_args(["--address", f"127.0.0.1/8:{_free_udp_port()}", "--instance", "999"])
    return Application.from_args(args)


async def _until(cond, timeout=10.0):
    for _ in range(int(timeout / 0.1)):
        if cond():
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.integration
def test_without_the_bbmd_the_routed_devices_are_out_of_reach(router, monkeypatch):
    async def run():
        app = _app(monkeypatch, bbmd=None)
        try:
            await asyncio.sleep(0.5)
            return await discovery.discover_devices(app, timeout=2)
        finally:
            app.close()

    assert asyncio.run(run()) == []


@pytest.mark.integration
def test_a_device_behind_a_router_is_discovered_and_polled_through_the_bbmd(router, monkeypatch, tmp_path):
    async def run():
        app = _app(monkeypatch, bbmd=router)
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        try:
            assert await _until(lambda: bacnet_network.network_status(app).get("registration") == "registered")
            status = bacnet_network.network_status(app)
            assert status["mode"] == "foreign" and status["bbmd"] == router

            devices = await discovery.discover_devices(app, timeout=3)
            by_instance = {d["device_instance"]: d for d in devices}
            assert by_instance[4242]["address"] == "2:5" and by_instance[4242]["network"] == 2
            assert by_instance[4243]["address"] == "2:6"
            assert by_instance[100]["address"] == router  # the router itself

            assert await bacnet_network.discover_routers(app, timeout=3) == [{"address": router, "networks": [2]}]
            assert bacnet_network.network_status(app)["routes"] == [{"network": 2, "router": router}]

            points = [{"object_identifier": "analog-value,1", "label": "zone-temp"},
                      {"object_identifier": "binary-value,1", "label": "fan-status"}]
            await manager.create_connection("2:5", 4242, "fbf/routed", points, poll_interval=0.5)
            assert await _until(lambda: {"fbf/routed/zone-temp", "fbf/routed/fan-status"} <= set(mqtt.readings))
            assert isinstance(mqtt.readings["fbf/routed/zone-temp"], float)

            # a forgotten route (what the poller does after repeated
            # failures) is found again with Who-Is-Router-To-Network
            assert bacnet_network.forget_route(app, "2:5") is True
            mqtt.readings.clear()
            assert await _until(lambda: "fbf/routed/zone-temp" in mqtt.readings)
        finally:
            for record in list(manager.connections):
                await manager.delete_connection(record)
            app.close()

    asyncio.run(run())


@pytest.mark.integration
def test_a_restored_routed_connection_finds_its_router_without_discovery(router, monkeypatch, tmp_path):
    """After a restart nothing is cached: the first read to 2:6 has to find
    the router for network 2 itself."""
    async def run():
        app = _app(monkeypatch, bbmd=router)
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        try:
            assert await _until(lambda: bacnet_network.network_status(app).get("registration") == "registered")
            await manager.create_connection("2:6", 4243, "fbf/routed6",
                                            [{"object_identifier": "analog-value,1", "label": "zone-temp"}], poll_interval=0.5)
            assert await _until(lambda: "fbf/routed6/zone-temp" in mqtt.readings)
        finally:
            for record in list(manager.connections):
                await manager.delete_connection(record)
            app.close()

    asyncio.run(run())
