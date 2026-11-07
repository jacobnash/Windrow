"""Baler#164's acceptance, live against windrow.mock_device
--intrinsic-reporting over loopback UDP (real bacpypes3 intrinsic reporting:
OutOfRange on analog-value,1, notification-class,1; no broker - published
messages are captured).

- With forward_alarms, Windrow adds itself to the device's notification
  class, and pushing the point past its high limit puts an alarm on
  `{prefix}/zone-temp/alarm` within a few seconds.
- A fresh Windrow started while the alarm is still active reports it from
  GetEventInformation, without waiting for a new transition.
- Deleting the connection takes Windrow out of the recipient-list again.
"""

import asyncio
import json
import socket
import subprocess
import sys
import time

import pytest
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.primitivedata import Null

from windrow import bacnet_alarms
from windrow.connection_manager import ConnectionManager


class CapturingMqtt:
    def __init__(self):
        self.alarms = []  # (monotonic time, topic, event)

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        if topic.endswith("/alarm") and payload:
            self.alarms.append((time.monotonic(), topic, json.loads(payload)["value"]))


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _windrow_app(port: int):
    args = SimpleArgumentParser().parse_args(["--address", f"127.0.0.1/8:{port}", "--instance", "999"])
    return bacnet_alarms.WindrowApplication.from_args(args)


async def _until(cond, timeout=10.0):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


POINTS = [{"object_identifier": "analog-value,1", "label": "zone-temp"}]


@pytest.mark.integration
def test_alarms_reach_mqtt_and_survive_a_restart(tmp_path):
    device_port, windrow_port = _free_udp_port(), _free_udp_port()
    address = f"127.0.0.1:{device_port}"
    proc = subprocess.Popen([sys.executable, "-m", "windrow.mock_device", "--address", f"127.0.0.1/8:{device_port}",
                             "--instance", "3456", "--intrinsic-reporting"])

    async def first_run():
        app = _windrow_app(windrow_port)
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        app.event_listeners.append(manager.on_event_notification)
        try:
            await asyncio.sleep(2)
            record = await manager.create_connection(address, 3456, "fbf/ahu-1", POINTS, poll_interval=1.0,
                                                     read_options={"forward_alarms": True})
            recipients = lambda: app.read_property(address, "notification-class,1", "recipient-list")
            for _ in range(100):
                if len(await recipients()) == 1:
                    break
                await asyncio.sleep(0.1)
            assert len(await recipients()) == 1
            pushed_at = time.monotonic()
            await app.write_property(address, "analog-value,1", "present-value", 95.0, priority=8)
            assert await _until(lambda: any(e.get("to_state") == "high-limit" for _, _, e in mqtt.alarms))
            hit = next((t, topic, e) for t, topic, e in mqtt.alarms if e.get("to_state") == "high-limit")
            return record, hit[0] - pushed_at, hit[1], hit[2]
        finally:
            app.close()

    async def restart_and_clear(record):
        app = _windrow_app(windrow_port)
        mqtt = CapturingMqtt()
        manager = ConnectionManager(app, mqtt, str(tmp_path / "c.json"))
        app.event_listeners.append(manager.on_event_notification)
        try:
            await manager.start()  # restores the forward_alarms connection from the state file
            assert await _until(lambda: any(e.get("event_state") == "high-limit" for _, _, e in mqtt.alarms))
            mqtt.alarms.clear()
            await app.write_property(address, "analog-value,1", "present-value", Null(()), priority=8)  # relinquish
            assert await _until(lambda: any(e.get("to_state") == "normal" for _, _, e in mqtt.alarms))
            await manager.delete_connection(record["id"])
            for _ in range(50):
                if len(await app.read_property(address, "notification-class,1", "recipient-list")) == 0:
                    break
                await asyncio.sleep(0.1)
            return len(await app.read_property(address, "notification-class,1", "recipient-list"))
        finally:
            app.close()

    try:
        time.sleep(0.5)
        record, latency, topic, event = asyncio.run(first_run())
        remaining = asyncio.run(restart_and_clear(record))
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    print(f"alarm on {topic} {latency * 1000:.0f} ms after the write: {event}")
    assert topic == "fbf/ahu-1/zone-temp/alarm"
    assert latency < 3.0
    assert event["event_type"] == "out-of-range" and event["from_state"] == "normal"
    assert remaining == 0
