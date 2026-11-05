"""
Drives the real handler directly (ephemeral port, no subprocess) through
the full discover -> learn -> create -> GET -> confirm MQTT -> DELETE ->
confirm-silence sequence against the live mock device and broker. This is
the literal proof of the "SkySpark-easy via API" claim.

HTTP calls run via asyncio.to_thread() rather than a bare blocking
urlopen() call on the test's own coroutine - the test coroutine runs on
the same loop that owns the Application/ConnectionManager, and the HTTP
server's request-handler threads reach that loop via
run_coroutine_threadsafe(); blocking the loop's thread directly inside the
test would deadlock the very calls it's waiting on.
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

from windrow.api import make_handler
from windrow.connection_manager import ConnectionManager
from windrow.device_registry import DeviceRegistry
from windrow.modbus_connection_manager import ModbusConnectionManager

DEVICE_ADDRESS = "192.168.128.63:47808"
DEVICE_INSTANCE = 3456


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
def test_full_discover_learn_create_list_delete_flow(tmp_path):
    topic_prefix = f"fbf/api-test-{int(time.time())}"

    async def run():
        app = _make_app(47816)
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

        sub = mqtt_test_util.subscriber_client()
        received = {}
        sub.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        base = f"http://127.0.0.1:{port}"

        try:
            await asyncio.sleep(1)  # let the Application's transport finish setup

            status, devices = await _request("POST", f"{base}/discover", {"address": DEVICE_ADDRESS, "timeout": 5.0})
            assert status == 200
            assert devices[0]["device_instance"] == DEVICE_INSTANCE

            status, points = await _request(
                "POST", f"{base}/learn", {"device_address": DEVICE_ADDRESS, "device_instance": DEVICE_INSTANCE}
            )
            assert status == 200
            by_id = {p["object_identifier"]: p for p in points}
            assert by_id["analog-value,1"]["label"] == "zone-temp"

            status, record = await _request(
                "POST",
                f"{base}/connections",
                {
                    "device_address": DEVICE_ADDRESS,
                    "device_instance": DEVICE_INSTANCE,
                    "topic_prefix": topic_prefix,
                    "poll_interval": 1.0,
                    "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
                },
            )
            assert status == 201
            connection_id = record["id"]

            value_topic = f"{topic_prefix}/zone-temp"
            for _ in range(30):
                sub.loop(timeout=0.1)
                if value_topic in received:
                    break
                await asyncio.sleep(0.1)
            assert value_topic in received
            assert isinstance(received[value_topic]["value"], float)

            status, listed = await _request("GET", f"{base}/connections")
            assert status == 200
            assert any(c["id"] == connection_id for c in listed)

            status, _ = await _request("DELETE", f"{base}/connections/{connection_id}")
            assert status == 204

            status, listed_after = await _request("GET", f"{base}/connections")
            assert not any(c["id"] == connection_id for c in listed_after)

            received.clear()
            await asyncio.sleep(1.5)  # > poll_interval
            for _ in range(10):
                sub.loop(timeout=0.1)
            assert value_topic not in received  # delete actually stopped polling, not just the record

            status, _ = await _request("DELETE", f"{base}/connections/{connection_id}")
            assert status == 404  # already gone
        finally:
            sub.disconnect()
            server.shutdown()
            await modbus_manager.close()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())
