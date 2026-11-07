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

from bacpypes3.apdu import GetEventInformationACK
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import GetEventInformationEventSummary, TimeStamp
from bacpypes3.local.analog import AnalogValueObjectCmd, AnalogValueObjectIR
from bacpypes3.local.binary import BinaryValueObjectCmd
from bacpypes3.local.object import Object as _LocalObject
from bacpypes3.object import NotificationClassObject

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


class MockNotificationClass(_LocalObject, NotificationClassObject):
    """bacpypes3 ships NotificationClassObject's definition but no local
    object for it; an intrinsic-reporting object sends its notifications to
    this one's recipient-list (Baler#164)."""


class MockApplication(Application):
    """Answers GetEventInformation (Baler#164), which bacpypes3 0.0.110's
    Application doesn't: every object not in a normal event state, or with
    a transition not yet acknowledged. One page; the mock has few objects."""

    async def do_GetEventInformationRequest(self, apdu) -> None:
        summaries = []
        for obj in self.objectIdentifier.values():
            if getattr(obj, "_event_algorithm", None) is None:
                continue
            acked = list(obj.ackedTransitions or [1, 1, 1])
            if str(obj.eventState) == "normal" and all(acked):
                continue
            summaries.append(GetEventInformationEventSummary(
                objectIdentifier=obj.objectIdentifier, eventState=obj.eventState,
                acknowledgedTransitions=acked, eventTimeStamps=list(obj.eventTimeStamps),
                notifyType=obj.notifyType, eventEnable=obj.eventEnable, eventPriorities=[3, 3, 3],
            ))
        await self.response(GetEventInformationACK(context=apdu, listOfEventSummaries=summaries, moreEvents=False))


def _make_object(spec: dict, intrinsic_reporting: bool = False):
    kwargs = dict(
        objectIdentifier=(spec["object_type"], spec["instance"]),
        objectName=spec["object_name"],
        presentValue=spec["initial_value"],
        statusFlags=[0, 0, 0, 0],
        description=spec.get("description") or "",
    )
    # Commandable (*Cmd) variants, like real controllers' value objects: a
    # 16-level priority array behind presentValue. The simulation writes
    # at priority 16 (bacpypes3's default for a plain presentValue set), so
    # an operator write at e.g. 8 overrides it until relinquished.
    if spec["object_type"] == "binaryValue":
        return BinaryValueObjectCmd(**kwargs)
    if spec.get("units"):
        kwargs["units"] = spec["units"]
    # COV (Baler#127): bacpypes3's analog COV detection compares against
    # covIncrement, and without one every presentValue change after a
    # SubscribeCOV raises inside the object - a write answers
    # operational-problem and the simulation task dies. Real controllers
    # ship a default increment too.
    kwargs["covIncrement"] = spec.get("cov_increment", 0.1)
    if intrinsic_reporting:
        # Intrinsic reporting (Baler#164): an out-of-range alarm 10 above or
        # below the starting value unless the spec says otherwise, through
        # notification-class,1. Write present-value past a limit (priority
        # 8) to raise one; relinquish it to clear it.
        start = float(spec["initial_value"])
        return AnalogValueObjectIR(
            **kwargs, eventState="normal", timeDelay=0, notificationClass=1,
            highLimit=float(spec.get("high_limit", start + 10)), lowLimit=float(spec.get("low_limit", start - 10)),
            deadband=1.0, limitEnable=[1, 1], eventEnable=[1, 1, 1], ackedTransitions=[1, 1, 1],
            notifyType="alarm", eventTimeStamps=[TimeStamp(sequenceNumber=0)] * 3, eventDetectionEnable=True,
        )
    return AnalogValueObjectCmd(**kwargs)


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
    def __init__(self, args=None, site_spec: list[dict] | None = None, app: Application | None = None,
                 intrinsic_reporting: bool = False):
        # `app`: an already-built Application, e.g. one on a virtual
        # network behind windrow.mock_router instead of on BACnet/IP
        self.app = app if app is not None else MockApplication.from_args(args)
        self.objects: list[tuple] = []

        if intrinsic_reporting:
            self.app.add_object(MockNotificationClass(
                objectIdentifier=("notification-class", 1), objectName="alarms", notificationClass=1,
                priority=[3, 3, 3], ackRequired=[True, True, False], recipientList=[],
            ))
        for spec in site_spec or _DEFAULT_SITE_SPEC:
            obj = _make_object(spec, intrinsic_reporting)
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
    parser.add_argument(
        "--intrinsic-reporting", action="store_true",
        help="analog objects alarm out of range (10 either side of their start) through notification-class,1",
    )
    args = parser.parse_args()
    site_spec = _load_site_spec(args.site_spec) if args.site_spec else None
    MockDevice(args, site_spec=site_spec, intrinsic_reporting=args.intrinsic_reporting)
    await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
