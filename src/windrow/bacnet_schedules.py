"""
Read BACnet Schedule and Calendar objects (Baler#165), for commissioning's
as-built vs spec check.

read_device_schedules reads every schedule and calendar object in a
device's object-list and turns them into plain JSON:

- weekly: seven days, Monday first, each a list of {time, value}
- exceptions: [{period, priority, times}], where period is
  {date}, {date_range}, {week_n_day} or {calendar} (a calendar object id)
- effective_period, default, priority_for_writing, present_value
- controls: what the schedule writes ("analog-value,3 present-value"), and
  the connection point label for it when the caller knows one
- calendars: {object, name, present_value, dates: [period, ...]}

Dates are `YYYY-MM-DD` with `*` for a wildcard field (`*-12-25` is every
Christmas); week-n-day is {month, week, weekday} with null for "any".

Each property is one ReadProperty, bounded (Baler#84), and an
Error/Reject/Abort answer (which bacpypes3 raises as a BaseException) is
handled. exception-schedule is often bigger than an unsegmented APDU on
small controllers: when reading it whole fails, it's read element by element
(array index 0 for the count, then 1..n). An optional property a device
doesn't have comes back null.
"""

from __future__ import annotations

from typing import Any

from windrow.bacnet_read import request
from windrow.bacnet_values import normalize_value

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


class ScheduleReadError(RuntimeError):
    """The device refused or didn't answer a schedule read."""


async def read_property(app, address: str, objid: str, prop: str, index: int | None = None,
                        timeout: float | None = None) -> Any:
    """One ReadProperty, optionally of one array element, cast to its
    BACnet type. Raises ScheduleReadError for an Error/Reject/Abort."""
    from bacpypes3.apdu import (
        ErrorRejectAbortNack,
        ReadPropertyACK,
        ReadPropertyRequest,
    )
    from bacpypes3.constructeddata import Array
    from bacpypes3.pdu import Address
    from bacpypes3.primitivedata import Unsigned

    addr = Address(address)
    vendor_info = await app.get_vendor_info(device_address=addr)
    parsed = await app.parse_object_identifier(objid, vendor_info=vendor_info)
    prop_id = (await app.parse_property_reference(prop, vendor_info=vendor_info)).propertyIdentifier
    req = ReadPropertyRequest(objectIdentifier=parsed, propertyIdentifier=prop_id, destination=addr)
    if index is not None:
        req.propertyArrayIndex = index
    answer = await request(app, req, timeout)
    if isinstance(answer, ErrorRejectAbortNack) or not isinstance(answer, ReadPropertyACK):
        raise ScheduleReadError(f"{address} answered {objid} {prop}{'' if index is None else f'[{index}]'} with {answer}")
    if answer.objectIdentifier != parsed or answer.propertyIdentifier != prop_id:
        raise ScheduleReadError(f"asked {address} for {objid} {prop}, got {answer.objectIdentifier} {answer.propertyIdentifier}")
    property_type = vendor_info.get_object_class(parsed[0]).get_property_type(prop_id)
    if index is not None and issubclass(property_type, Array):
        property_type = Unsigned if index == 0 else property_type._subtype
    return answer.propertyValue.cast_out(property_type)


async def _optional(app, address, objid, prop):
    try:
        return await read_property(app, address, objid, prop)
    except ScheduleReadError:
        return None


async def _exception_schedule(app, address, objid) -> list:
    try:
        return list(await read_property(app, address, objid, "exception-schedule") or [])
    except ScheduleReadError:
        pass
    # too big for one answer (or not supported whole): element by element
    try:
        count = int(await read_property(app, address, objid, "exception-schedule", index=0))
    except ScheduleReadError:
        return []  # no exception schedule at all
    return [await read_property(app, address, objid, "exception-schedule", index=i) for i in range(1, count + 1)]


def date_text(date) -> str:
    year, month, day, _dow = tuple(date)
    y = "*" if year == 255 else str(year + 1900)
    m = "*" if month == 255 else (
        {13: "odd", 14: "even"}.get(month, f"{month:02d}"))
    d = "*" if day == 255 else ({32: "last", 33: "odd", 34: "even"}.get(day, f"{day:02d}"))
    return f"{y}-{m}-{d}"


def time_text(time) -> str:
    hour, minute, second, hundredths = tuple(time)
    parts = ["*" if v == 255 else f"{v:02d}" for v in (hour, minute, second)]
    text = ":".join(parts)
    if hundredths not in (0, 255):
        text += f".{hundredths:02d}"
    return text


def scheduled_value(value) -> Any:
    """A schedule value (an AnyAtomic, or already a primitive) as JSON. Null
    is "relinquish" in a schedule, so it stays None."""
    from bacpypes3.primitivedata import Enumerated

    if hasattr(value, "get_value"):
        value = value.get_value()
    if type(value) is Enumerated:
        # a bare enumeration, which is what a schedule holds - its name isn't
        # known without the controlled object's type; _binary_values names it
        return int(value)
    return normalize_value(value)


def _time_values(values) -> list[dict]:
    return [{"time": time_text(tv.time), "value": scheduled_value(tv.value)} for tv in values or []]


def _week_n_day(value) -> dict:
    month, week, weekday = bytes(value)
    return {
        "month": None if month == 255 else month,
        "week": None if week == 255 else week,  # 1-5 = days 1-7, 8-14, ...; 6 = last 7 days of the month
        "weekday": None if weekday == 255 else WEEKDAYS[weekday - 1],
    }


def calendar_entry(entry) -> dict:
    if entry.date is not None:
        return {"date": date_text(entry.date)}
    if entry.dateRange is not None:
        return {"date_range": [date_text(entry.dateRange.startDate), date_text(entry.dateRange.endDate)]}
    if entry.weekNDay is not None:
        return {"week_n_day": _week_n_day(entry.weekNDay)}
    return {}


def special_event(event) -> dict:
    period = event.period
    out = {"calendar": str(period.calendarReference)} if period.calendarReference is not None else calendar_entry(
        period.calendarEntry)
    return {"period": out, "priority": int(event.eventPriority), "times": _time_values(event.listOfTimeValues)}


def _reference(ref, labels: dict[str, str]) -> dict:
    target = str(ref.objectIdentifier)
    out = {"object": target, "property": str(ref.propertyIdentifier)}
    if ref.propertyArrayIndex is not None:
        out["index"] = int(ref.propertyArrayIndex)
    if ref.deviceIdentifier is not None:
        out["device"] = str(ref.deviceIdentifier)
    if ref.deviceIdentifier is None and target in labels:
        out["label"] = labels[target]
    return out


def _binary_values(schedule: dict) -> None:
    """A schedule that drives binary objects carries enumerations: 1/0 on the
    wire. Show them as active/inactive, the way a poll of those points does."""
    def fix(v):
        return {1: "active", 0: "inactive"}.get(v, v) if isinstance(v, int) and not isinstance(v, bool) else v

    for key in ("present_value", "default"):
        schedule[key] = fix(schedule[key])
    for day in (schedule["weekly"] or {}).values():
        for tv in day:
            tv["value"] = fix(tv["value"])
    for event in schedule["exceptions"]:
        for tv in event["times"]:
            tv["value"] = fix(tv["value"])


async def read_schedule(app, address: str, objid: str, labels: dict[str, str] | None = None) -> dict:
    weekly = await _optional(app, address, objid, "weekly-schedule")
    effective = await _optional(app, address, objid, "effective-period")
    refs = await _optional(app, address, objid, "list-of-object-property-references")
    schedule = {
        "object": objid,
        "name": normalize_value(await _optional(app, address, objid, "object-name")),
        "present_value": scheduled_value(await read_property(app, address, objid, "present-value")),
        "default": scheduled_value(await _optional(app, address, objid, "schedule-default")),
        "priority_for_writing": normalize_value(await _optional(app, address, objid, "priority-for-writing")),
        "effective_period": None if effective is None else [date_text(effective.startDate), date_text(effective.endDate)],
        "weekly": None if weekly is None else {
            WEEKDAYS[i]: _time_values(day.daySchedule) for i, day in enumerate(list(weekly)[:7])
        },
        "exceptions": [special_event(e) for e in await _exception_schedule(app, address, objid)],
        "controls": [_reference(r, labels or {}) for r in (refs or [])],
    }
    if schedule["controls"] and all(c["object"].startswith("binary") for c in schedule["controls"]):
        _binary_values(schedule)
    return schedule


async def read_calendar(app, address: str, objid: str) -> dict:
    dates = await _optional(app, address, objid, "date-list")
    return {
        "object": objid,
        "name": normalize_value(await _optional(app, address, objid, "object-name")),
        "present_value": bool(await _optional(app, address, objid, "present-value")),
        "dates": [calendar_entry(e) for e in (dates or [])],
    }


async def read_device_schedules(app, address: str, device_instance: int, labels: dict[str, str] | None = None) -> dict:
    """Every schedule and calendar on the device. `labels` maps object ids
    the caller polls to their point labels, so `controls` can name them."""
    object_list = await read_property(app, address, f"device,{device_instance}", "object-list")
    schedules, calendars = [], []
    for objid in object_list:
        kind = str(objid[0])
        if kind == "schedule":
            schedules.append(await read_schedule(app, address, f"schedule,{objid[1]}", labels))
        elif kind == "calendar":
            calendars.append(await read_calendar(app, address, f"calendar,{objid[1]}"))
    return {"schedules": schedules, "calendars": calendars}
