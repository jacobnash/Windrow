"""
Simulated BACnet/IP device for testing Windrow without real hardware.

Exposes read-only points that change over time, so the bridge and the
drift-detector both have something real to observe. Adapted from
BACpypes3's own mini-device sample (bacpypes3/samples/mini-device-revisited.py).

Defaults to the original two hardcoded points (zone-temp, fan-status) so the
existing live integration tests (test_discovery.py, test_connection_manager.py)
keep working unchanged. Pass --site-spec to load a much larger generated
object list instead - see windrow.mock_hospital, which builds one.
"""

import asyncio
import json
import random

from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.app import Application
from bacpypes3.local.analog import AnalogValueObject
from bacpypes3.local.binary import BinaryValueObject

INTERVAL = 5.0

_DEFAULT_SITE_SPEC = [
    {
        "object_type": "analogValue",
        "instance": 1,
        "object_name": "zone-temp",
        "description": "Simulated zone temperature",
        "units": "degreesFahrenheit",
        "initial_value": 68.0,
        "behavior": "sensor",
    },
    {
        "object_type": "binaryValue",
        "instance": 1,
        "object_name": "fan-status",
        "description": "Simulated fan on/off status",
        "units": None,
        "initial_value": "inactive",
        "behavior": "status",
    },
]


def _make_object(spec: dict):
    kwargs = dict(
        objectIdentifier=(spec["object_type"], spec["instance"]),
        objectName=spec["object_name"],
        presentValue=spec["initial_value"],
        statusFlags=[0, 0, 0, 0],
        description=spec.get("description") or "",
    )
    if spec["object_type"] == "binaryValue":
        return BinaryValueObject(**kwargs)
    if spec.get("units"):
        kwargs["units"] = spec["units"]
    return AnalogValueObject(**kwargs)


def _next_value(value, behavior: str):
    """One drift rule per behavior class - sensors wander, setpoints mostly
    hold, status/command points flip occasionally (command less often than
    status, since a command is a human/schedule decision, not a live
    measurement)."""
    if behavior == "sensor":
        return round(value + random.uniform(-0.5, 0.5), 2)
    if behavior == "setpoint":
        return round(value + random.uniform(-0.1, 0.1), 2) if random.random() < 0.1 else value
    if behavior == "status":
        return ("inactive" if value == "active" else "active") if random.random() < 0.2 else value
    if behavior == "command":
        return ("inactive" if value == "active" else "active") if random.random() < 0.05 else value
    return value


class MockDevice:
    def __init__(self, args, site_spec: list[dict] | None = None):
        self.app = Application.from_args(args)
        self.objects: list[tuple] = []

        for spec in site_spec or _DEFAULT_SITE_SPEC:
            obj = _make_object(spec)
            self.app.add_object(obj)
            self.objects.append((obj, spec["behavior"]))

        asyncio.create_task(self._simulate())

    async def _simulate(self) -> None:
        # ponytail: one shared loop over every object rather than one task
        # per object - fine at hundreds of objects, revisit if per-point
        # poll_interval-style staggering is ever needed here too.
        while True:
            await asyncio.sleep(INTERVAL)
            for obj, behavior in self.objects:
                obj.presentValue = _next_value(obj.presentValue, behavior)


def _load_site_spec(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)


async def main() -> None:
    parser = SimpleArgumentParser()
    parser.add_argument(
        "--site-spec",
        default=None,
        help="path to a JSON list of BACnet object specs (see windrow.mock_hospital); omit for the original 2-point demo device",
    )
    args = parser.parse_args()
    site_spec = _load_site_spec(args.site_spec) if args.site_spec else None
    MockDevice(args, site_spec=site_spec)
    await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
