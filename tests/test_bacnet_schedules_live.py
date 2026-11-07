"""Baler#165's acceptance, live: the schedules API returns the weekly and
exception entries windrow.mock_device --schedules was configured with
(bacpypes3's own ScheduleObject, over loopback UDP)."""

import asyncio
import socket
import subprocess
import sys
import time

import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import mock_device
from windrow.connection_manager import ConnectionManager


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.integration
def test_schedules_match_what_the_mock_was_configured_with(tmp_path):
    port = _free_udp_port()
    proc = subprocess.Popen([sys.executable, "-m", "windrow.mock_device", "--address", f"127.0.0.1/8:{port}",
                             "--instance", "3456", "--schedules"])

    async def run():
        args = SimpleArgumentParser().parse_args(["--address", f"127.0.0.1/8:{_free_udp_port()}", "--instance", "999"])
        app = Application.from_args(args)
        try:
            await asyncio.sleep(0.5)
            manager = ConnectionManager(app, None, str(tmp_path / "c.json"))
            manager.connections = {"c1": {"id": "c1", "device_address": f"127.0.0.1:{port}", "device_instance": 3456,
                                          "points": [{"object_identifier": "binary-value,1", "label": "fan-status"}]}}
            return await manager.connection_schedules("c1")
        finally:
            app.close()

    try:
        time.sleep(2.5)
        result = asyncio.run(run())
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    (schedule,), (calendar,) = result["schedules"], result["calendars"]
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    expected_weekly = {
        day: [{"time": f"{t}:00", "value": v} for t, v in entries] for day, entries in zip(days, mock_device.MOCK_WEEKLY)
    }
    assert schedule["weekly"] == expected_weekly
    assert schedule["exceptions"] == [
        {"period": {"calendar": "calendar,1"}, "priority": 1, "times": [{"time": "00:00:00", "value": "inactive"}]},
        {"period": {"week_n_day": {"month": 11, "week": 4, "weekday": "thursday"}}, "priority": 10,
         "times": [{"time": "07:00:00", "value": "active"}, {"time": "12:00:00", "value": "inactive"}]},
    ]
    assert schedule["controls"] == [{"object": "binary-value,1", "property": "present-value", "label": "fan-status"}]
    assert schedule["default"] == "inactive" and schedule["priority_for_writing"] == 10
    assert calendar["dates"] == [{"date": d} for d in mock_device.MOCK_HOLIDAYS]
