"""
Live against the already-running mock device (windrow.mock_device, instance
3456 at 192.168.128.63:47808) - no bacpypes3 mocking. Uses directed
discovery (address=...) rather than global broadcast: a locally-originated
broadcast isn't delivered back to another process on the *same* machine
over a real physical interface (confirmed - no local loopback for
broadcast on macOS/BSD), which is an environment limitation, not something
discover_devices() needs to special-case. Global broadcast uses the exact
same code path and works identically against a real second host.
"""

import asyncio

import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow.discovery import discover_devices, learn_points

DEVICE_ADDRESS = "192.168.128.63:47808"
DEVICE_INSTANCE = 3456


def _make_app() -> Application:
    # Application.from_args() schedules async setup work (asyncio.ensure_future)
    # that requires a running loop - it can't be constructed in a plain
    # sync pytest fixture, only inside the same asyncio.run() as the test.
    parser = SimpleArgumentParser()
    args = parser.parse_args(["--address", "192.168.128.63/24:47811"])
    return Application.from_args(args)


@pytest.mark.integration
def test_discover_devices_finds_the_real_mock_device():
    async def run():
        app = _make_app()
        try:
            await asyncio.sleep(1)  # let transport setup finish
            return await discover_devices(app, address=DEVICE_ADDRESS, timeout=5.0)
        finally:
            app.close()

    devices = asyncio.run(run())

    assert len(devices) == 1
    assert devices[0]["device_instance"] == DEVICE_INSTANCE


@pytest.mark.integration
def test_learn_points_returns_real_points_with_correct_units():
    async def run():
        app = _make_app()
        try:
            await asyncio.sleep(1)
            return await learn_points(app, DEVICE_ADDRESS, DEVICE_INSTANCE)
        finally:
            app.close()

    points = asyncio.run(run())
    by_id = {p["object_identifier"]: p for p in points}

    assert by_id["analog-value,1"]["label"] == "zone-temp"
    assert by_id["analog-value,1"]["units"] == "degrees-fahrenheit"
    assert isinstance(by_id["analog-value,1"]["present_value"], float)

    # binary-value,1 ("fan-status") has no units= on the mock object - this
    # is the real, always-exercised ErrorType-in-RPM-results case, not a
    # hypothetical: bacpypes3 embeds an ErrorType for the missing property
    # rather than raising, and learn_points must report it as None, not
    # crash the whole call over one missing field on one point.
    assert by_id["binary-value,1"]["label"] == "fan-status"
    assert by_id["binary-value,1"]["units"] is None
    assert by_id["binary-value,1"]["present_value"] in ("active", "inactive")
