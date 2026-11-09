"""
Write BACnet Schedule and Calendar objects (Windrow#26) - the inverse of
bacnet_schedules' read format, so what Baler reads it can change and write
back (Baler#213's audited schedule writes, and their automatic revert).

The shapes are bacnet_schedules' exactly:

- weekly: all seven days, Monday first, each [{time, value}]
- exceptions: [{period, priority, times: [{time, value}]}], period one of
  {date}, {date_range: [start, end]}, {week_n_day: {month, week, weekday}}
  or {calendar: "calendar,N"}
- default: a value; effective_period: [start, end]
- a calendar's dates: [period, ...] (no calendar references)

Dates are `YYYY-MM-DD` with `*` for any, and `odd`/`even` months, `last`/
`odd`/`even` days; times `HH:MM[:SS[.hh]]`. A value is null (relinquish),
a number, true/false, or active/inactive - encoded for what the schedule
drives (schedule_value_kind): an enumeration for binary objects, Unsigned
for multi-state, Real for analog.

validate_schedule/validate_calendar check everything before anything is
sent, so a malformed command never half-writes. Writes themselves are
WriteProperty requests, bounded like every other request (Baler#84).
"""

from __future__ import annotations

import re
from typing import Any

from windrow.bacnet_read import request
from windrow.bacnet_schedules import WEEKDAYS

MAX_TIMES = 24  # per day / per exception
MAX_EXCEPTIONS = 64
MAX_DATES = 128

_TIME = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{2}))?)?$")
_MONTHS = {"odd": 13, "even": 14}
_DAYS = {"last": 32, "odd": 33, "even": 34}


class ScheduleWriteError(ValueError):
    """A malformed schedule/calendar write, or one the device refused."""


# --- parsing the read format back into BACnet values ---


def parse_date(text, where: str = "date"):
    from bacpypes3.primitivedata import Date

    parts = str(text).split("-") if isinstance(text, str) else []
    if len(parts) != 3:
        raise ScheduleWriteError(f"{where}: a date is YYYY-MM-DD (with * for any field), got {text!r}")
    y, m, d = parts
    try:
        year = 255 if y == "*" else int(y) - 1900
        month = 255 if m == "*" else _MONTHS.get(m) or int(m)
        day = 255 if d == "*" else _DAYS.get(d) or int(d)
    except ValueError:
        raise ScheduleWriteError(f"{where}: a date is YYYY-MM-DD (with * for any field), got {text!r}") from None
    if not (year == 255 or 0 <= year <= 254) or not (month == 255 or 1 <= month <= 14) or not (day == 255 or 1 <= day <= 34):
        raise ScheduleWriteError(f"{where}: {text!r} is out of range")
    return Date((year, month, day, 255))


def parse_time(text, where: str = "time"):
    from bacpypes3.primitivedata import Time

    match = _TIME.match(text) if isinstance(text, str) else None
    if not match:
        raise ScheduleWriteError(f"{where}: a time is HH:MM[:SS[.hh]], got {text!r}")
    hour, minute, second, hundredths = (int(g) if g else 0 for g in match.groups())
    if hour > 23 or minute > 59 or second > 59:
        raise ScheduleWriteError(f"{where}: {text!r} is out of range")
    return Time((hour, minute, second, hundredths))


def _week_n_day(spec, where: str) -> bytes:
    if not isinstance(spec, dict) or set(spec) - {"month", "week", "weekday"}:
        raise ScheduleWriteError(f"{where}: week_n_day is {{month, week, weekday}} (null for any)")
    month, week, weekday = spec.get("month"), spec.get("week"), spec.get("weekday")
    if month is not None and (isinstance(month, bool) or not isinstance(month, int) or not 1 <= month <= 14):
        raise ScheduleWriteError(f"{where}: week_n_day month is 1-14 or null")
    if week is not None and (isinstance(week, bool) or not isinstance(week, int) or not 1 <= week <= 6):
        raise ScheduleWriteError(f"{where}: week_n_day week is 1-6 or null")
    if weekday is not None and weekday not in WEEKDAYS:
        raise ScheduleWriteError(f"{where}: week_n_day weekday is monday..sunday or null")
    return bytes([255 if month is None else month, 255 if week is None else week,
                  255 if weekday is None else WEEKDAYS.index(weekday) + 1])


def calendar_entry(period, where: str):
    """{date} | {date_range} | {week_n_day} -> a CalendarEntry."""
    from bacpypes3.basetypes import CalendarEntry, DateRange

    if not isinstance(period, dict) or len(period) != 1:
        raise ScheduleWriteError(f"{where}: a period is one of {{date}}, {{date_range}}, {{week_n_day}}")
    (kind, value), = period.items()
    if kind == "date":
        return CalendarEntry(date=parse_date(value, where))
    if kind == "date_range":
        if not isinstance(value, list) or len(value) != 2:
            raise ScheduleWriteError(f"{where}: date_range is [start, end]")
        return CalendarEntry(dateRange=DateRange(startDate=parse_date(value[0], where), endDate=parse_date(value[1], where)))
    if kind == "week_n_day":
        return CalendarEntry(weekNDay=_week_n_day(value, where))
    raise ScheduleWriteError(f"{where}: unknown period {kind!r}")


def _special_event_period(period, where: str):
    from bacpypes3.basetypes import SpecialEventPeriod
    from bacpypes3.primitivedata import ObjectIdentifier

    if isinstance(period, dict) and set(period) == {"calendar"}:
        ref = period["calendar"]
        if not isinstance(ref, str) or not re.fullmatch(r"calendar,\d+", ref):
            raise ScheduleWriteError(f"{where}: calendar is a calendar object id like \"calendar,1\"")
        return SpecialEventPeriod(calendarReference=ObjectIdentifier(ref))
    return SpecialEventPeriod(calendarEntry=calendar_entry(period, where))


def schedule_value_kind(schedule: dict) -> str:
    """binary | multistate | analog | boolean - how this schedule's values
    are encoded, from the objects it controls (as bacnet_schedules reads
    them), else from its current default's type."""
    kinds = set()
    for control in schedule.get("controls") or []:
        obj = str(control.get("object", ""))
        if obj.startswith("binary"):
            kinds.add("binary")
        elif obj.startswith("multi-state"):
            kinds.add("multistate")
        elif obj.startswith("analog"):
            kinds.add("analog")
    if len(kinds) == 1:
        return kinds.pop()
    default = schedule.get("default")
    if default in ("active", "inactive"):
        return "binary"
    if isinstance(default, bool):
        return "boolean"
    if isinstance(default, int):
        return "multistate"
    return "analog"


def encode_value(value, kind: str, where: str):
    """A read-format value -> the atomic a TimeValue/schedule-default holds."""
    from bacpypes3.primitivedata import Boolean, Enumerated, Null, Real, Unsigned

    if value is None:
        return Null(())  # relinquish
    if kind == "binary":
        text = str(value).strip().lower() if isinstance(value, str) else value
        if text in ("active", "on", True, 1):
            return Enumerated(1)
        if text in ("inactive", "off", False, 0):
            return Enumerated(0)
        raise ScheduleWriteError(f"{where}: a binary schedule takes active/inactive, got {value!r}")
    if kind == "multistate":
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 1 or not float(value).is_integer():
            raise ScheduleWriteError(f"{where}: a multi-state schedule takes a state number >= 1, got {value!r}")
        return Unsigned(int(value))
    if kind == "boolean":
        if not isinstance(value, bool):
            raise ScheduleWriteError(f"{where}: this schedule takes true/false, got {value!r}")
        return Boolean(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduleWriteError(f"{where}: an analog schedule takes a number, got {value!r}")
    return Real(float(value))


def _times(entries, kind: str, where: str) -> list:
    from bacpypes3.basetypes import TimeValue

    if not isinstance(entries, list) or len(entries) > MAX_TIMES:
        raise ScheduleWriteError(f"{where}: a list of up to {MAX_TIMES} {{time, value}}")
    out = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or set(entry) != {"time", "value"}:
            raise ScheduleWriteError(f"{where}[{i}]: each entry is {{time, value}}")
        out.append(TimeValue(time=parse_time(entry["time"], f"{where}[{i}].time"),
                             value=encode_value(entry["value"], kind, f"{where}[{i}].value")))
    return out


# --- whole writes, validated up front ---

SCHEDULE_FIELDS = ("effective_period", "weekly", "exceptions", "default")  # written in this order


def validate_schedule(body: dict, schedule: dict) -> list[tuple[str, Any]]:
    """[(BACnet property, value)] for every field of a write_schedule body
    that's present, encoded for `schedule` (as read now). Raises
    ScheduleWriteError before anything is written."""
    from bacpypes3.basetypes import DailySchedule, DateRange, SpecialEvent
    from bacpypes3.constructeddata import ArrayOf

    unknown = set(body) - set(SCHEDULE_FIELDS)
    if unknown:
        raise ScheduleWriteError(f"unknown schedule field(s): {', '.join(sorted(unknown))}")
    kind = schedule_value_kind(schedule)
    writes: list[tuple[str, Any]] = []
    if "effective_period" in body:
        period = body["effective_period"]
        if not isinstance(period, list) or len(period) != 2:
            raise ScheduleWriteError("effective_period is [start, end]")
        writes.append(("effective-period", DateRange(startDate=parse_date(period[0], "effective_period"),
                                                     endDate=parse_date(period[1], "effective_period"))))
    if "weekly" in body:
        weekly = body["weekly"]
        if not isinstance(weekly, dict) or set(weekly) != set(WEEKDAYS):
            raise ScheduleWriteError("weekly has all seven days, monday..sunday, each a list of {time, value}")
        days = [DailySchedule(daySchedule=_times(weekly[day], kind, f"weekly.{day}")) for day in WEEKDAYS]
        writes.append(("weekly-schedule", ArrayOf(DailySchedule)(days)))
    if "exceptions" in body:
        exceptions = body["exceptions"]
        if not isinstance(exceptions, list) or len(exceptions) > MAX_EXCEPTIONS:
            raise ScheduleWriteError(f"exceptions is a list of up to {MAX_EXCEPTIONS}")
        events = []
        for i, event in enumerate(exceptions):
            where = f"exceptions[{i}]"
            if not isinstance(event, dict) or set(event) - {"period", "priority", "times"} or "period" not in event:
                raise ScheduleWriteError(f"{where} is {{period, priority, times}}")
            priority = event.get("priority", 16)
            if isinstance(priority, bool) or not isinstance(priority, int) or not 1 <= priority <= 16:
                raise ScheduleWriteError(f"{where}.priority is 1-16")
            events.append(SpecialEvent(period=_special_event_period(event["period"], f"{where}.period"),
                                       listOfTimeValues=_times(event.get("times") or [], kind, f"{where}.times"),
                                       eventPriority=priority))
        writes.append(("exception-schedule", ArrayOf(SpecialEvent)(events)))
    if "default" in body:
        writes.append(("schedule-default", encode_value(body["default"], kind, "default")))
    if not writes:
        raise ScheduleWriteError("nothing to write: give weekly, exceptions, default or effective_period")
    return writes


def validate_calendar(body: dict) -> list[tuple[str, Any]]:
    from bacpypes3.basetypes import CalendarEntry
    from bacpypes3.constructeddata import ListOf

    unknown = set(body) - {"dates"}
    if unknown:
        raise ScheduleWriteError(f"unknown calendar field(s): {', '.join(sorted(unknown))}")
    dates = body.get("dates")
    if not isinstance(dates, list) or len(dates) > MAX_DATES:
        raise ScheduleWriteError(f"dates is a list of up to {MAX_DATES} periods")
    entries = [calendar_entry(period, f"dates[{i}]") for i, period in enumerate(dates)]
    return [("date-list", ListOf(CalendarEntry)(entries))]


async def write_property(app, address: str, objid: str, prop: str, value, timeout: float | None = None) -> None:
    """One WriteProperty, bounded. Raises ScheduleWriteError if the device
    refuses it (an Error/Reject/Abort answer) and TimeoutError if it
    doesn't answer."""
    from bacpypes3.apdu import ErrorRejectAbortNack, SimpleAckPDU, WritePropertyRequest
    from bacpypes3.constructeddata import Any as AnyValue
    from bacpypes3.pdu import Address

    addr = Address(address)
    vendor_info = await app.get_vendor_info(device_address=addr)
    parsed = await app.parse_object_identifier(objid, vendor_info=vendor_info)
    prop_id = (await app.parse_property_reference(prop, vendor_info=vendor_info)).propertyIdentifier
    answer = await request(app, WritePropertyRequest(
        objectIdentifier=parsed, propertyIdentifier=prop_id, propertyValue=AnyValue(value), destination=addr,
    ), timeout)
    if isinstance(answer, ErrorRejectAbortNack) or not isinstance(answer, SimpleAckPDU):
        raise ScheduleWriteError(f"{address} refused writing {objid} {prop}: {answer}")
