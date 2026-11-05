"""
Live against the real handler (ephemeral port, no subprocess) covering the
new-since-BACnet-only surface: the periodic BACnet discovery scan feeding
/devices, and the Modbus discover -> create-connection -> credential flow.
Same driving pattern as test_api.py.
"""

import asyncio
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import mqtt_test_util
import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from cryptography.fernet import Fernet

from windrow import discovery
from windrow.api import make_handler
from windrow.connection_manager import ConnectionManager
from windrow.device_registry import DeviceRegistry
from windrow.modbus_connection_manager import ModbusConnectionManager

DEVICE_ADDRESS = "192.168.128.63:47808"
DEVICE_INSTANCE = 3456


async def _fake_discover(devices: list[dict]) -> list[dict]:
    return devices


def _make_app(bind_port: int) -> Application:
    parser = SimpleArgumentParser()
    args = parser.parse_args(["--address", f"192.168.128.63/24:{bind_port}"])
    return Application.from_args(args)


async def _request(method: str, url: str, body: dict | None = None):
    def do_request():
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, (json.loads(raw) if raw else None)

    return await asyncio.to_thread(do_request)


@pytest.mark.integration
def test_periodic_bacnet_scan_populates_devices_then_provisioned(tmp_path, monkeypatch):
    """discovery.discover_devices() itself (real Who-Is/I-Am broadcast) is
    already covered live by test_discovery.py - what this test is actually
    proving is the periodic-scan loop's own wiring (tick -> reconcile ->
    sleep -> repeat, correctly reflecting pending/provisioned across a real
    connection's lifecycle through the real HTTP API). Stubbed to a fixed
    result rather than a real broadcast: a same-host broadcast Who-Is is a
    documented, genuinely flaky loopback case on macOS/BSD (see
    openapi.yaml's /discover description) - non-deterministic in a way
    that would make this test flaky for a reason that has nothing to do
    with whether the periodic-scan feature itself is correct."""
    monkeypatch.setenv("WINDROW_CREDENTIAL_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(
        discovery, "discover_devices", lambda app, **kwargs: _fake_discover([{"device_instance": DEVICE_INSTANCE, "address": "192.168.128.63", "vendor_id": 999}])
    )

    async def run():
        app = _make_app(47819)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))
        await manager.start()
        modbus_manager = ModbusConnectionManager(mqtt_client, str(tmp_path / "modbus-connections.json"))
        await modbus_manager.start()
        device_registry = DeviceRegistry(str(tmp_path / "devices.json"))
        await device_registry.start()

        loop = asyncio.get_event_loop()
        scan_task = loop.create_task(device_registry.run_periodic_bacnet_scan(app, manager, 1.0))

        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(manager, modbus_manager, device_registry, loop))
        server.daemon_threads = True
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        base = f"http://127.0.0.1:{port}"

        try:
            await asyncio.sleep(1)  # let the Application's transport finish setup

            # First scan tick should find the always-on mock device as pending.
            status, devices = await _wait_for(lambda: _request("GET", f"{base}/devices"), lambda r: len(r[1]) > 0)
            assert status == 200
            found = next(d for d in devices if d["device_instance"] == DEVICE_INSTANCE)
            assert found["status"] == "pending"
            assert found["protocol"] == "bacnet"
            assert found["default_credential_flag"] is None  # BACnet never gets a default-cred check
            device_id = found["id"]

            # Setting a credential on the still-pending device works and redacts on GET.
            status, cred_resp = await _request(
                "PATCH", f"{base}/devices/{device_id}/credential", {"username": "admin", "password": "sup3r-secret"}
            )
            assert status == 200
            assert cred_resp == {"username": "admin", "password": "sup3r-secret"}

            status, devices = await _request("GET", f"{base}/devices")
            found = next(d for d in devices if d["id"] == device_id)
            assert found["credential"] == {"username": "admin", "password": None}

            # Provisioning a connection for it flips status to provisioned on the next scan tick.
            status, _ = await _request(
                "POST",
                f"{base}/connections",
                {
                    "device_address": DEVICE_ADDRESS,
                    "device_instance": DEVICE_INSTANCE,
                    "topic_prefix": f"fbf/devices-api-test-{int(time.time())}",
                    "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
                },
            )
            assert status == 201

            status, devices = await _wait_for(
                lambda: _request("GET", f"{base}/devices"),
                lambda r: next(d for d in r[1] if d["id"] == device_id)["status"] == "provisioned",
            )
            found = next(d for d in devices if d["id"] == device_id)
            assert found["status"] == "provisioned"
            assert found["connection_id"] is not None
        finally:
            scan_task.cancel()
            server.shutdown()
            await modbus_manager.close()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())


@pytest.mark.integration
def test_modbus_discover_create_list_delete_flow(mock_modbus_device, tmp_path, monkeypatch):
    monkeypatch.setenv("WINDROW_CREDENTIAL_KEY", Fernet.generate_key().decode())
    topic_prefix = f"fbf/modbus-api-test-{int(time.time())}"

    async def run():
        app = _make_app(47820)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))
        await manager.start()
        modbus_manager = ModbusConnectionManager(mqtt_client, str(tmp_path / "modbus-connections.json"))
        await modbus_manager.start()
        device_registry = DeviceRegistry(str(tmp_path / "devices.json"))
        await device_registry.start()

        loop = asyncio.get_event_loop()
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(manager, modbus_manager, device_registry, loop))
        server.daemon_threads = True
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        base = f"http://127.0.0.1:{port}"

        try:
            status, devices = await _request("POST", f"{base}/modbus/discover", {"cidr": "127.0.0.1/32", "port": mock_modbus_device})
            assert status == 200
            assert devices == [
                {"host": "127.0.0.1", "port": mock_modbus_device, "vendor": "Windrow Mocks", "product_code": "MOCK-MB-1", "revision": "1.0"}
            ]

            status, record = await _request(
                "POST",
                f"{base}/modbus/connections",
                {
                    "host": "127.0.0.1",
                    "port": mock_modbus_device,
                    "topic_prefix": topic_prefix,
                    "poll_interval": 1.0,
                    "points": {"zone-temp": 0},
                    "credential": {"username": "admin", "password": "sup3r-secret"},
                },
            )
            assert status == 201
            assert record["credential"] == {"username": "admin", "password": None}  # redacted immediately, even on create
            connection_id = record["id"]

            status, listed = await _request("GET", f"{base}/modbus/connections")
            assert status == 200
            assert any(c["id"] == connection_id and c["credential"]["password"] is None for c in listed)

            status, cred_resp = await _request(
                "PATCH", f"{base}/modbus/connections/{connection_id}/credential", {"username": "admin2", "password": "new-secret"}
            )
            assert status == 200
            assert cred_resp == {"username": "admin2", "password": "new-secret"}

            status, _ = await _request("DELETE", f"{base}/modbus/connections/{connection_id}")
            assert status == 204

            status, listed_after = await _request("GET", f"{base}/modbus/connections")
            assert not any(c["id"] == connection_id for c in listed_after)

            status, _ = await _request("DELETE", f"{base}/modbus/connections/{connection_id}")
            assert status == 404  # already gone
        finally:
            server.shutdown()
            await modbus_manager.close()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())


async def _wait_for(request_coro_factory, predicate, attempts: int = 30, delay: float = 0.5):
    """Polls request_coro_factory() until predicate(result) is true or the
    attempt budget runs out - used for the periodic scan's own timing
    (it ticks on a 1s interval in this test), not this project's usual
    tight MQTT-loop polling."""
    result = None
    for _ in range(attempts):
        result = await request_coro_factory()
        try:
            if predicate(result):
                return result
        except StopIteration:
            pass
        await asyncio.sleep(delay)
    return result
