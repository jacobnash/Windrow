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
import datetime
import json
import random

from bacpypes3.apdu import GetEventInformationACK, ReadRangeACK
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import (
    GetEventInformationEventSummary,
    LogRecord,
    LogRecordLogDatum,
    TimeStamp,
)
from bacpypes3.constructeddata import Any, SequenceOf
from bacpypes3.errors import ExecutionError
from bacpypes3.local.analog import AnalogValueObjectCmd, AnalogValueObjectIR
from bacpypes3.local.binary import BinaryValueObjectCmd
from bacpypes3.local.object import Object as _LocalObject
from bacpypes3.local.schedule import ScheduleObject
from bacpypes3.object import CalendarObject, NotificationClassObject, TrendLogObject
from bacpypes3.primitivedata import Enumerated

from windrow import bacnet_trend

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


class MockTrendLog(_LocalObject, TrendLogObject):
    """A TrendLog that polls one object's presentValue every log interval
    into a ring buffer (Baler#129), served by MockApplication's ReadRange.
    Records are kept as (sequence number, LogRecord, device-local datetime);
    the device's clock is this host's local time, like bacpypes3's own
    DeviceObject.localDate/localTime."""

    def __init__(self, source, interval: float, buffer_size: int = 10_000, **kwargs):
        super().__init__(
            logDeviceObjectProperty={"objectIdentifier": source.objectIdentifier, "propertyIdentifier": "present-value"},
            logInterval=int(interval * 100), bufferSize=buffer_size, enable=True, stopWhenFull=False,
            recordCount=0, totalRecordCount=0, loggingType="polled", statusFlags=[0, 0, 0, 0],
            eventState="normal", **kwargs,
        )
        self._source = source
        self._interval = interval
        self._buffer_size = buffer_size
        self._records: list[tuple[int, LogRecord, datetime.datetime]] = []
        self._next_sequence = 1

    def log_now(self) -> None:
        value = self._source.presentValue
        if self._source.objectIdentifier[0] in ("binary-value", "binary-input", "binary-output") or str(
            self._source.objectIdentifier[0]
        ).startswith("binary"):
            datum = LogRecordLogDatum(enumValue=1 if str(value) == "active" else 0)
        else:
            datum = LogRecordLogDatum(realValue=float(value))
        now = datetime.datetime.now()  # noqa: DTZ005 - a BACnet device logs its local wall-clock time
        record = LogRecord(timestamp=bacnet_trend.to_bacnet_datetime(now), logDatum=datum, statusFlags=[0, 0, 0, 0])
        self._records.append((self._next_sequence, record, now))
        self._next_sequence += 1
        del self._records[:-self._buffer_size]
        self.recordCount = len(self._records)
        self.totalRecordCount = self._next_sequence - 1

    async def run(self) -> None:
        while True:
            self.log_now()
            await asyncio.sleep(self._interval)


class MockApplication(Application):
    """Services bacpypes3 0.0.110's Application doesn't answer. GetEventInformation
    (Baler#164): every object not in a normal event state, or with a transition
    not yet acknowledged, in one page - the mock has few objects.

    ReadRange: bacpypes3 0.0.110's Application has no ReadRange server (its
    do_ReadRangeRequest raises NotImplementedError); this one answers
    ReadRange on a MockTrendLog's log-buffer, by position, sequence number
    or time, per Clause 15.8."""

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

    async def do_ReadRangeRequest(self, apdu) -> None:
        obj = self.get_object_id(apdu.objectIdentifier)
        if not isinstance(obj, MockTrendLog):
            raise ExecutionError(errorClass="object", errorCode="unknownObject")
        if str(apdu.propertyIdentifier) != "log-buffer":
            raise ExecutionError(errorClass="property", errorCode="propertyIsNotAList")
        records = obj._records
        rng = apdu.range
        indexes: list[int]
        if rng is None:
            indexes = list(range(len(records)))
        elif rng.byTime is not None:
            ref = bacnet_trend.from_bacnet_datetime(rng.byTime.referenceTime)
            count = int(rng.byTime.count)
            if count > 0:
                indexes = [i for i, (_, _, when) in enumerate(records) if when > ref][:count]
            else:
                indexes = [i for i, (_, _, when) in enumerate(records) if when < ref][count:]
        elif rng.bySequenceNumber is not None:
            ref, count = int(rng.bySequenceNumber.referenceSequenceNumber), int(rng.bySequenceNumber.count)
            if count > 0:
                indexes = [i for i, (seq, _, _) in enumerate(records) if seq >= ref][:count]
            else:
                indexes = [i for i, (seq, _, _) in enumerate(records) if seq <= ref][count:]
        else:
            ref, count = int(rng.byPosition.referenceIndex), int(rng.byPosition.count)  # 1-based
            start, stop = (ref - 1, ref - 1 + count) if count > 0 else (ref + count, ref)
            indexes = list(range(max(0, start), min(len(records), stop)))
        items = [records[i][1] for i in indexes]
        more = bool(indexes) and (
            indexes[-1] < len(records) - 1 if (rng is None or _forward(rng)) else indexes[0] > 0
        )
        await self.response(ReadRangeACK(
            context=apdu,
            objectIdentifier=apdu.objectIdentifier,
            propertyIdentifier=apdu.propertyIdentifier,
            resultFlags=[bool(indexes) and indexes[0] == 0, bool(indexes) and indexes[-1] == len(records) - 1, more],
            itemCount=len(items),
            itemData=Any(SequenceOf(LogRecord)(items)),
            firstSequenceNumber=records[indexes[0]][0] if indexes else None,
        ))


def _forward(rng) -> bool:
    for choice in (rng.byTime, rng.bySequenceNumber, rng.byPosition):
        if choice is not None:
            return int(choice.count) > 0
    return True


class MockCalendar(_LocalObject, CalendarObject):
    """bacpypes3 has CalendarObject's definition but no local object for it."""

    async def _post_init(self):
        # the local Object's version looks up a notificationClass, which a
        # Calendar doesn't have (it raises AttributeError in a stray task)
        return None


# The mock's schedule (Baler#165), as the spec says it should be: occupied
# 07:00-18:00 on weekdays, all day off on the holidays in calendar,1, and
# an early close the fourth Thursday of November.
MOCK_WEEKLY = [[("07:00", "active"), ("18:00", "inactive")]] * 5 + [[], []]
MOCK_HOLIDAYS = ["2026-12-25", "*-01-01"]


def _time(text: str):
    hour, minute = (int(x) for x in text.split(":"))
    return (hour, minute, 0, 0)


def _binary(value: str):
    return Enumerated(1 if value == "active" else 0)


def _mock_schedule_objects(target) -> list:
    """schedule,1 driving `target`'s present-value, and calendar,1."""
    from bacpypes3.basetypes import (
        CalendarEntry,
        DailySchedule,
        DateRange,
        SpecialEvent,
        TimeValue,
    )
    from bacpypes3.primitivedata import Date

    def day(entries):
        return DailySchedule(daySchedule=[TimeValue(time=_time(t), value=_binary(v)) for t, v in entries])

    def date(text):
        year, month, dom = text.split("-")
        return Date((255 if year == "*" else int(year) - 1900, int(month), int(dom), 255))

    calendar = MockCalendar(objectIdentifier=("calendar", 1), objectName="holidays", presentValue=False,
                            dateList=[CalendarEntry(date=date(d)) for d in MOCK_HOLIDAYS])
    schedule = ScheduleObject(
        objectIdentifier=("schedule", 1), objectName="occupancy", presentValue=_binary("inactive"),
        effectivePeriod=DateRange(startDate=date("2000-01-01"), endDate=date("2099-12-31")),
        weeklySchedule=[day(entries) for entries in MOCK_WEEKLY],
        exceptionSchedule=[
            SpecialEvent(period={"calendarReference": calendar.objectIdentifier},
                         listOfTimeValues=[TimeValue(time=_time("00:00"), value=_binary("inactive"))], eventPriority=1),
            SpecialEvent(period={"calendarEntry": CalendarEntry(weekNDay=bytes([11, 4, 4]))},
                         listOfTimeValues=[TimeValue(time=_time("07:00"), value=_binary("active")),
                                           TimeValue(time=_time("12:00"), value=_binary("inactive"))], eventPriority=10),
        ],
        scheduleDefault=_binary("inactive"),
        listOfObjectPropertyReferences=[{"objectIdentifier": target.objectIdentifier, "propertyIdentifier": "present-value"}],
        priorityForWriting=10, statusFlags=[0, 0, 0, 0], reliability="no-fault-detected", outOfService=False,
    )
    return [calendar, schedule]


class MockDevice:
    def __init__(self, args=None, site_spec: list[dict] | None = None, trend_log_interval: float | None = None,
                 app: Application | None = None, intrinsic_reporting: bool = False, schedules: bool = False):
        # `app`: an already-built Application, e.g. one on a virtual
        # network behind windrow.mock_router instead of on BACnet/IP (that
        # one has no ReadRange or GetEventInformation server)
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

        # One TrendLog per object, trend-log,1..N in object order, when asked
        # for: off by default so /learn on the plain mock lists only its points.
        self.trend_logs: list[MockTrendLog] = []
        if trend_log_interval:
            for n, (obj, _behavior) in enumerate(self.objects, start=1):
                trend = MockTrendLog(obj, trend_log_interval, objectIdentifier=("trend-log", n),
                                     objectName=f"{obj.objectName}-trend")
                self.app.add_object(trend)
                self.trend_logs.append(trend)
                asyncio.create_task(trend.run())

        # A schedule and a holiday calendar (Baler#165), driving the first
        # binary object; off by default, like the trend logs.
        if schedules:
            target = next((obj for obj, _ in self.objects if str(obj.objectIdentifier[0]).startswith("binary")), None)
            if target is None:
                raise SystemExit("--schedules needs a binary object for the schedule to drive")
            for obj in _mock_schedule_objects(target):
                self.app.add_object(obj)

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
    parser.add_argument(
        "--trend-log-interval", type=float, default=None,
        help="seconds; adds a TrendLog (trend-log,1..N, in object order) logging each object's present-value",
    )
    parser.add_argument(
        "--schedules", action="store_true",
        help="adds schedule,1 (weekdays 07:00-18:00, holiday and early-close exceptions) driving the first binary "
        "object, and calendar,1 (the holidays)",
    )
    args = parser.parse_args()
    site_spec = _load_site_spec(args.site_spec) if args.site_spec else None
    MockDevice(args, site_spec=site_spec, trend_log_interval=args.trend_log_interval,
               intrinsic_reporting=args.intrinsic_reporting, schedules=args.schedules)
    await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
