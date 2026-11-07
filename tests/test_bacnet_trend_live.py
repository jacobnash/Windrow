"""Baler#129's trend-log backfill, live against windrow.mock_device's
TrendLog and ReadRange over loopback UDP (no broker - readings captured).

Windrow "was down" for ~8 s: the marks file says its last reading was then,
while the mock device kept logging every 0.2 s. On start, the point's first
good read backfills the gap from the device's trend log - several ReadRange
pages - and publishes it not retained, with the records' own timestamps."""

import asyncio
import json
import socket
import subprocess
import sys
import time

import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import connection_manager as cm


class CapturingMqtt:
    def __init__(self):
        self.published = []  # (topic, payload dict, retain)

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        if not topic.endswith("/tags") and payload:
            self.published.append((topic, json.loads(payload), retain))


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.integration
def test_restart_backfills_the_gap_from_the_devices_trend_log(tmp_path):
    port = _free_udp_port()
    proc = subprocess.Popen([sys.executable, "-m", "windrow.mock_device", "--address", f"127.0.0.1/8:{port}",
                             "--instance", "3456", "--trend-log-interval", "0.2"])
    state_file = tmp_path / "windrow-connections.json"
    points = [{"object_identifier": "analog-value,1", "label": "zone-temp", "trend_log": "trend-log,1"},
              {"object_identifier": "binary-value,1", "label": "fan-status", "trend_log": "trend-log,2"}]

    async def run():
        args = SimpleArgumentParser().parse_args(["--address", f"127.0.0.1/8:{_free_udp_port()}", "--instance", "999"])
        app = Application.from_args(args)
        mqtt = CapturingMqtt()
        try:
            await asyncio.sleep(2)  # the mock logs meanwhile
            manager = cm.ConnectionManager(app, mqtt, str(state_file))
            record = await manager.create_connection(f"127.0.0.1:{port}", 3456, "fbf/trend", points, poll_interval=1.0)
            # pretend this connection last published 8 s ago, then restart polling
            await cm.stop_task(manager.tasks.pop(record["id"]))
            gap_start = time.time() - 8
            manager._marks = {f"{record['id']}/zone-temp": gap_start, f"{record['id']}/fan-status": gap_start}
            await asyncio.sleep(8)
            mqtt.published.clear()
            restarted_at = time.time()
            manager._start_poll_task(record)
            for _ in range(100):
                history = [p for t, p, retain in mqtt.published if not retain and t == "fbf/trend/zone-temp"]
                if len(history) > 30:
                    break
                await asyncio.sleep(0.1)
            await asyncio.sleep(1)
            for connection_id in list(manager.connections):
                await manager.delete_connection(connection_id)
            return mqtt.published, gap_start, restarted_at
        finally:
            app.close()

    try:
        published, gap_start, restarted_at = asyncio.run(run())
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    temps = [p for t, p, retain in published if not retain and t == "fbf/trend/zone-temp"]
    fans = [p for t, p, retain in published if not retain and t == "fbf/trend/fan-status"]
    print(f"backfilled {len(temps)} zone-temp and {len(fans)} fan-status readings")
    # ~16 s of 0.2 s records between the mark and the first read: several 20-record pages
    assert len(temps) >= 30
    assert all(gap_start < p["ts"] < restarted_at + 2 for p in temps)
    assert [p["ts"] for p in temps] == sorted(p["ts"] for p in temps)
    assert all(isinstance(p["value"], float) for p in temps)
    assert fans and all(p["value"] in ("active", "inactive") for p in fans)
    # the live readings are still the retained ones
    assert any(retain and t == "fbf/trend/zone-temp" for t, p, retain in published)
