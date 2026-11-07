"""Baler#165: reading BACnet Schedule and Calendar objects. No device needed
- the live version against windrow.mock_device --schedules is in
test_bacnet_schedules_live.py."""

import asyncio

import pytest
from bacpypes3.basetypes import (
    CalendarEntry,
    DailySchedule,
    DateRange,
    DeviceObjectPropertyReference,
    SpecialEvent,
    TimeValue,
)
from bacpypes3.primitivedata import Date, Enumerated, ObjectIdentifier, Real

from windrow import bacnet_schedules as bs


def test_dates_and_times_with_wildcards():
    assert bs.date_text(Date((126, 12, 25, 255))) == "2026-12-25"
    assert bs.date_text(Date((255, 1, 1, 255))) == "*-01-01"
    assert bs.date_text(Date((255, 255, 32, 255))) == "*-*-last"
    assert bs.time_text((7, 30, 0, 0)) == "07:30:00"
    assert bs.time_text((7, 30, 0, 50)) == "07:30:00.50"


def test_calendar_entries():
    assert bs.calendar_entry(CalendarEntry(date=Date((126, 7, 4, 255)))) == {"date": "2026-07-04"}
    assert bs.calendar_entry(CalendarEntry(dateRange=DateRange(startDate=Date((126, 12, 24, 255)),
                                                               endDate=Date((127, 1, 2, 255))))) == {
        "date_range": ["2026-12-24", "2027-01-02"]}
    assert bs.calendar_entry(CalendarEntry(weekNDay=bytes([11, 4, 4]))) == {
        "week_n_day": {"month": 11, "week": 4, "weekday": "thursday"}}
    assert bs.calendar_entry(CalendarEntry(weekNDay=bytes([255, 255, 1]))) == {
        "week_n_day": {"month": None, "week": None, "weekday": "monday"}}


def _tv(time, value):
    return TimeValue(time=time, value=value)


class FakeDevice:
    """Answers read_property from a dict; can refuse exception-schedule whole
    (too big for one APDU on a small controller)."""

    def __init__(self, props, whole_exceptions=True):
        self.props = props
        self.whole_exceptions = whole_exceptions
        self.reads = []

    async def read_property(self, app, address, objid, prop, index=None, timeout=None):
        self.reads.append((objid, prop, index))
        if prop == "exception-schedule":
            events = self.props.get((objid, prop))
            if events is None:
                raise bs.ScheduleReadError("unknown property")
            if index is None:
                if not self.whole_exceptions:
                    raise bs.ScheduleReadError("abort: segmentation-not-supported")
                return events
            return len(events) if index == 0 else events[index - 1]
        if (objid, prop) not in self.props:
            raise bs.ScheduleReadError(f"no {prop}")
        return self.props[(objid, prop)]


def _device(whole_exceptions=True, target="binary-value,1"):
    weekday = DailySchedule(daySchedule=[_tv((7, 0, 0, 0), Enumerated(1)), _tv((18, 0, 0, 0), Enumerated(0))])
    events = [
        SpecialEvent(period={"calendarReference": ObjectIdentifier("calendar,1")},
                     listOfTimeValues=[_tv((0, 0, 0, 0), Enumerated(0))], eventPriority=1),
        SpecialEvent(period={"calendarEntry": CalendarEntry(date=Date((126, 7, 3, 255)))},
                     listOfTimeValues=[_tv((7, 0, 0, 0), Enumerated(1)), _tv((12, 0, 0, 0), Enumerated(0))], eventPriority=8),
    ]
    s = "schedule,1"
    return FakeDevice({
        ("device,5", "object-list"): [ObjectIdentifier("device,5"), ObjectIdentifier("binary-value,1"),
                                      ObjectIdentifier(s), ObjectIdentifier("calendar,1")],
        (s, "object-name"): "occupancy",
        (s, "present-value"): Enumerated(1),
        (s, "schedule-default"): Enumerated(0),
        (s, "priority-for-writing"): 10,
        (s, "effective-period"): DateRange(startDate=Date((126, 1, 1, 255)), endDate=Date((126, 12, 31, 255))),
        (s, "weekly-schedule"): [weekday] * 5 + [DailySchedule(daySchedule=[])] * 2,
        (s, "exception-schedule"): events,
        (s, "list-of-object-property-references"): [
            DeviceObjectPropertyReference(objectIdentifier=ObjectIdentifier(target), propertyIdentifier="present-value")],
        ("calendar,1", "object-name"): "holidays",
        ("calendar,1", "present-value"): False,
        ("calendar,1", "date-list"): [CalendarEntry(date=Date((126, 12, 25, 255))), CalendarEntry(date=Date((255, 1, 1, 255)))],
    }, whole_exceptions)


def _read(device, monkeypatch, labels=None):
    monkeypatch.setattr(bs, "read_property", device.read_property)
    return asyncio.run(bs.read_device_schedules(None, "10.0.0.5", 5, labels))


def test_a_device_schedule_and_calendar_as_json(monkeypatch):
    result = _read(_device(), monkeypatch, labels={"binary-value,1": "occ-cmd"})
    (schedule,), (calendar,) = result["schedules"], result["calendars"]
    assert schedule["name"] == "occupancy" and schedule["priority_for_writing"] == 10
    assert schedule["present_value"] == "active" and schedule["default"] == "inactive"  # binary target: named
    assert schedule["effective_period"] == ["2026-01-01", "2026-12-31"]
    assert schedule["weekly"]["monday"] == [{"time": "07:00:00", "value": "active"}, {"time": "18:00:00", "value": "inactive"}]
    assert schedule["weekly"]["sunday"] == []
    assert schedule["exceptions"] == [
        {"period": {"calendar": "calendar,1"}, "priority": 1, "times": [{"time": "00:00:00", "value": "inactive"}]},
        {"period": {"date": "2026-07-03"}, "priority": 8,
         "times": [{"time": "07:00:00", "value": "active"}, {"time": "12:00:00", "value": "inactive"}]},
    ]
    assert schedule["controls"] == [{"object": "binary-value,1", "property": "present-value", "label": "occ-cmd"}]
    assert calendar == {"object": "calendar,1", "name": "holidays", "present_value": False,
                        "dates": [{"date": "2026-12-25"}, {"date": "*-01-01"}]}


def test_an_exception_schedule_too_big_for_one_answer_is_read_element_by_element(monkeypatch):
    device = _device(whole_exceptions=False)
    schedule = _read(device, monkeypatch)["schedules"][0]
    assert len(schedule["exceptions"]) == 2
    assert ("schedule,1", "exception-schedule", 0) in device.reads
    assert ("schedule,1", "exception-schedule", 2) in device.reads


def test_optional_properties_a_device_lacks_come_back_null(monkeypatch):
    device = _device()
    for prop in ("weekly-schedule", "exception-schedule", "effective-period", "schedule-default"):
        device.props.pop(("schedule,1", prop))
    schedule = _read(device, monkeypatch)["schedules"][0]
    assert schedule["weekly"] is None and schedule["exceptions"] == [] and schedule["effective_period"] is None


def test_an_analog_schedule_keeps_its_numbers(monkeypatch):
    device = _device(target="analog-value,3")
    device.props[("schedule,1", "weekly-schedule")] = [DailySchedule(daySchedule=[_tv((6, 0, 0, 0), Real(72.0))])] * 7
    device.props[("schedule,1", "present-value")] = Real(72.0)
    schedule = _read(device, monkeypatch)["schedules"][0]
    assert schedule["weekly"]["friday"] == [{"time": "06:00:00", "value": 72.0}]
    assert schedule["present_value"] == 72.0
    assert schedule["exceptions"][0]["times"][0]["value"] == 0  # an enum on an analog schedule: left as a number


def test_a_device_that_refuses_the_object_list_is_an_error(monkeypatch):
    device = _device()
    del device.props[("device,5", "object-list")]
    with pytest.raises(bs.ScheduleReadError):
        _read(device, monkeypatch)


def test_connection_schedules_names_the_connections_points(tmp_path, monkeypatch):
    from windrow import connection_manager as cm

    seen = {}

    async def fake(app, address, instance, labels=None):
        seen.update(address=address, instance=instance, labels=labels)
        return {"schedules": [], "calendars": []}

    monkeypatch.setattr(cm.bacnet_schedules, "read_device_schedules", fake)
    manager = cm.ConnectionManager(None, None, str(tmp_path / "c.json"))
    manager.connections = {"c1": {"id": "c1", "device_address": "10.0.0.5", "device_instance": 5, "points": [
        {"object_identifier": "binaryValue,1", "label": "occ-cmd"}]}}
    assert asyncio.run(manager.connection_schedules("c1")) == {"schedules": [], "calendars": []}
    assert seen == {"address": "10.0.0.5", "instance": 5, "labels": {"binary-value,1": "occ-cmd"}}
    assert asyncio.run(manager.connection_schedules("nope")) is None
