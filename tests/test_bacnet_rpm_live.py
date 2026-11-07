"""Baler#127's acceptance, live against windrow.mock_device over loopback
UDP (a real bacpypes3 device, real BACnet/IP packets; no broker - published
readings are captured). Spawns its own mock device on 127.0.0.1, so it
doesn't need the docker network's 192.168.128.63 device the other
integration tests use.

- the mock hospital site (~600 points, ~80 connections, one device) polled
  with ReadPropertyMultiple sends at least 10x fewer requests per poll
  cycle than one ReadProperty per point;
- a subscribed (COV) point publishes a change within one second.
"""

import asyncio
import json
import socket
import subprocess
import sys
import time

import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import mock_hospital
from windrow.connection_manager import ConnectionManager


class CapturingMqtt:
    def __init__(self):
        self.published = []  # (monotonic time, topic, value)

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        if topic.endswith("/tags"):
            return  # tag records, not readings
        body = json.loads(payload) if payload else None
        if isinstance(body, dict) and "value" in body:
            self.published.append((time.monotonic(), topic, body["value"]))


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def mock_site(tmp_path):
    """(device address, equipment) for a mock_device subprocess serving the
    mock hospital site on loopback."""
    site_spec, equipment = mock_hospital.generate_site()
    spec_path = tmp_path / "site.json"
    spec_path.write_text(json.dumps(site_spec))
    port = _free_udp_port()
    proc = subprocess.Popen([
        sys.executable, "-m", "windrow.mock_device", "--address", f"127.0.0.1/8:{port}", "--instance", "3456",
        "--site-spec", str(spec_path),
    ])
    try:
        time.sleep(3)  # ~600 objects to build
        yield f"127.0.0.1:{port}", equipment
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _client_app() -> Application:
    args = SimpleArgumentParser().parse_args(["--address", f"127.0.0.1/8:{_free_udp_port()}", "--instance", "999"])
    return Application.from_args(args)


async def _requests_per_cycle(address, equipment, tmp_path, read_multiple: bool, cycles: int = 3, interval: float = 2.0):
    app = _client_app()
    mqtt = CapturingMqtt()
    manager = ConnectionManager(app, mqtt, str(tmp_path / f"c-{read_multiple}.json"))
    try:
        await asyncio.sleep(0.5)
        for equip in equipment:
            await manager.create_connection(address, 3456, equip["topic_prefix"], equip["points"], poll_interval=interval,
                                            read_options={"read_multiple": read_multiple})
        await asyncio.sleep(interval * 1.5)  # first reads spread in
        before = dict(manager.stats)
        published_before = len(mqtt.published)
        await asyncio.sleep(interval * cycles)
        sent = sum(manager.stats[k] - before.get(k, 0) for k in ("read_property", "read_property_multiple"))
        return sent / cycles, (len(mqtt.published) - published_before) / cycles
    finally:
        for record in list(manager.connections):
            await manager.delete_connection(record)
        app.close()


@pytest.mark.integration
def test_rpm_cuts_requests_per_cycle_at_least_10x_on_the_mock_hospital(mock_site, tmp_path):
    address, equipment = mock_site
    point_count = sum(len(e["points"]) for e in equipment)

    single, _ = asyncio.run(_requests_per_cycle(address, equipment, tmp_path, read_multiple=False))
    batched, batched_published = asyncio.run(_requests_per_cycle(address, equipment, tmp_path, read_multiple=True))
    print(f"{point_count} points, {len(equipment)} connections: {single:.0f} requests/cycle single, {batched:.1f} batched")
    assert single >= point_count * 0.9  # sanity: one read per point
    assert batched_published >= point_count * 0.9  # every point still reported each cycle
    assert single / batched >= 10


@pytest.mark.integration
def test_a_cov_point_publishes_a_change_within_a_second(mock_site, tmp_path):
    address, _ = mock_site

    async def run():
        app = _client_app()
        writer = _client_app()
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        try:
            await asyncio.sleep(0.5)
            # a setpoint the simulation rarely moves, slow poll so only COV can be fast
            await manager.create_connection(address, 3456, "fbf/cov", [{"object_identifier": "analog-value,1", "label": "p"}],
                                            poll_interval=30.0, read_options={"subscribe_cov": True, "cov_poll_interval": 300.0})
            for _ in range(100):
                if manager.read_stats()["cov_subscriptions"]:
                    break
                await asyncio.sleep(0.1)
            assert manager.read_stats()["cov_subscriptions"] == 1
            await asyncio.sleep(0.5)
            mqtt.published.clear()
            changed_at = time.monotonic()
            await writer.write_property(address, "analog-value,1", "present-value", 12.5, priority=8)
            for _ in range(20):
                hits = [t for t, topic, value in mqtt.published if topic == "fbf/cov/p" and value == 12.5]
                if hits:
                    break
                await asyncio.sleep(0.05)
            assert hits, mqtt.published
            latency = hits[0] - changed_at
            print(f"COV latency {latency * 1000:.0f} ms")
            assert latency < 1.0
            assert manager.stats["read_property"] + manager.stats["read_property_multiple"] <= 1  # not polling for it
        finally:
            for record in list(manager.connections):
                await manager.delete_connection(record)
            await asyncio.sleep(0.2)
            app.close()
            writer.close()

    asyncio.run(run())


@pytest.mark.integration
def test_batched_polling_survives_a_device_restart(tmp_path, monkeypatch):
    """Baler#84 with RPM on: kill the device mid-poll, publish nothing while
    it's gone, resume on its own when it's back on the same address."""
    from windrow import bacnet_read

    monkeypatch.setattr(bacnet_read, "READ_TIMEOUT", 1.0)
    port = _free_udp_port()
    cmd = [sys.executable, "-m", "windrow.mock_device", "--address", f"127.0.0.1/8:{port}", "--instance", "3456"]
    points = [{"object_identifier": "analog-value,1", "label": "zone-temp"},
              {"object_identifier": "binary-value,1", "label": "fan-status"}]

    async def run():
        proc = await asyncio.to_thread(subprocess.Popen, cmd)
        app = _client_app()
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        try:
            await asyncio.sleep(2)
            await manager.create_connection(f"127.0.0.1:{port}", 3456, "fbf/r", points, poll_interval=0.5)
            await asyncio.sleep(2)
            assert {topic for _, topic, _ in mqtt.published} == {"fbf/r/zone-temp", "fbf/r/fan-status"}
            assert manager.stats["read_property_multiple"] > 0

            proc.terminate()
            proc.wait(timeout=5)
            await asyncio.sleep(1.5)
            mqtt.published.clear()
            await asyncio.sleep(3)
            assert mqtt.published == []  # nothing, not nulls, while it's down

            proc = await asyncio.to_thread(subprocess.Popen, cmd)
            for _ in range(200):
                if {topic for _, topic, _ in mqtt.published} == {"fbf/r/zone-temp", "fbf/r/fan-status"}:
                    break
                await asyncio.sleep(0.1)
            assert {topic for _, topic, _ in mqtt.published} == {"fbf/r/zone-temp", "fbf/r/fan-status"}
            assert all(value is not None for _, _, value in mqtt.published)
        finally:
            for record in list(manager.connections):
                await manager.delete_connection(record)
            app.close()
            proc.terminate()
            proc.wait(timeout=5)

    asyncio.run(run())
