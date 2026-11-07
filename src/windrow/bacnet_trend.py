"""
Backfill from BACnet TrendLog objects (Baler#129).

A controller keeps logging while Windrow is down or can't reach it; its
TrendLog buffer is the only copy of those readings. This reads the records
logged after a given time with ReadRange (by time, paged by `moreItems`) and
turns them into (unix ts, value) pairs, which connection_manager publishes,
not retained, under the point's own topic with the record's own timestamp.
Baler's ingest is idempotent on (point, ts), so reading an overlapping
window twice is harmless.

Same guarantees as bacnet_read: every request is bounded by
WINDROW_READ_TIMEOUT (bacpypes3's own read_range has no timeout - Baler#84)
and the answer must be for the object and property asked (Baler#61).

Timestamps: a LogRecord's timestamp is the device's local wall-clock time,
with no zone. device_utc_offset reads the device's local-date/local-time
and compares it with our UTC clock (rounded to 15 minutes, so clock drift
doesn't count as an offset); a device that won't say is assumed to keep
this host's local time. A backfill that spans a DST change is off by the
change for the records on the far side of it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from windrow.bacnet_read import READ_TIMEOUT, MismatchedResponse

# Records per ReadRange. A LogRecord with a Real is ~20 bytes encoded, so 20
# fits even a 480-byte MS/TP APDU unsegmented.
PAGE_SIZE = 20


class TrendReadError(RuntimeError):
    """The device answered a ReadRange with an error, reject or abort."""


def to_bacnet_datetime(local: dt.datetime):
    from bacpypes3.basetypes import DateTime
    from bacpypes3.primitivedata import Date, Time

    return DateTime(
        date=Date((local.year - 1900, local.month, local.day, local.isoweekday())),
        time=Time((local.hour, local.minute, local.second, local.microsecond // 10000)),
    )


def from_bacnet_datetime(value) -> dt.datetime | None:
    """A naive datetime (device-local), or None for a timestamp with
    wildcards (255) in it, which can't be placed."""
    year, month, day, _dow = tuple(value.date)
    hour, minute, second, hundredths = tuple(value.time)
    if 255 in (year, month, day, hour, minute) or month > 12 or day > 31:
        return None
    second = 0 if second == 255 else second
    hundredths = 0 if hundredths == 255 else hundredths
    return dt.datetime(year + 1900, month, day, hour, minute, second, hundredths * 10000)  # noqa: DTZ001 - device-local, zone unknown here


def host_utc_offset() -> dt.timedelta:
    return dt.datetime.now().astimezone().utcoffset() or dt.timedelta(0)


async def device_utc_offset(app, address, device_instance: int, timeout: float | None = None) -> dt.timedelta | None:
    """The device's local time minus UTC, from its local-date/local-time
    (rounded to 15 min), or None if it won't say."""
    from bacpypes3.apdu import ErrorRejectAbortNack
    from bacpypes3.basetypes import DateTime

    from windrow.bacnet_read import read_present_value

    device = f"device,{device_instance}"
    try:
        local_date = await read_present_value(app, address, device, "local-date", timeout)
        local_time = await read_present_value(app, address, device, "local-time", timeout)
        now_utc = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        device_now = from_bacnet_datetime(DateTime(date=local_date, time=local_time))
    # bacpypes3 raises an Error answer, and it's a BaseException
    except (Exception, ErrorRejectAbortNack):  # noqa: BLE001 - no clock from this device: the caller uses the host's zone
        return None
    if device_now is None:
        return None
    quarter_hours = round((device_now - now_utc).total_seconds() / 900)
    return dt.timedelta(minutes=15 * quarter_hours)


def datum_value(log_datum, object_type: str) -> Any:
    """A log record's value in the shape a poll of the same point would
    publish (normalize_value's): Real -> float, a binary point's enum ->
    active/inactive, other enums/unsigned/signed -> int. Status, time-change
    and failure records carry no reading: None."""
    for choice in ("realValue", "unsignedValue", "signedValue"):
        value = getattr(log_datum, choice, None)
        if value is not None:
            return float(value) if choice == "realValue" else int(value)
    value = getattr(log_datum, "enumValue", None)
    if value is not None:
        if object_type.startswith("binary"):
            return "active" if int(value) else "inactive"
        return int(value)
    value = getattr(log_datum, "booleanValue", None)
    if value is not None:
        return bool(value)
    return None


async def read_records_after(app, address, trend_log: str, after_local: dt.datetime, count: int = PAGE_SIZE,
                             timeout: float | None = None) -> tuple[list, bool]:
    """One ReadRange by time: up to `count` records logged after
    `after_local` (device time). Returns ([(naive local datetime, log
    datum)], more_items)."""
    from bacpypes3.apdu import ErrorRejectAbortNack, ReadRangeACK, ReadRangeRequest
    from bacpypes3.basetypes import LogRecord, PropertyIdentifier, Range, RangeByTime
    from bacpypes3.constructeddata import SequenceOf
    from bacpypes3.pdu import Address

    if isinstance(address, str):
        address = Address(address)
    vendor_info = await app.get_vendor_info(device_address=address)
    objid = await app.parse_object_identifier(trend_log, vendor_info=vendor_info)
    prop = PropertyIdentifier("log-buffer")
    request = ReadRangeRequest(
        objectIdentifier=objid,
        propertyIdentifier=prop,
        range=Range(byTime=RangeByTime(referenceTime=to_bacnet_datetime(after_local), count=count)),
        destination=address,
    )
    try:
        response = await asyncio.wait_for(app.request(request), READ_TIMEOUT if timeout is None else timeout)
    except ErrorRejectAbortNack as answer:  # bacpypes3 raises these, and they're BaseExceptions
        response = answer
    if isinstance(response, ErrorRejectAbortNack):
        raise TrendReadError(f"{address} answered ReadRange {trend_log} with {response!r}")
    if not isinstance(response, ReadRangeACK):
        raise TrendReadError(f"{address} answered ReadRange {trend_log} with {response!r}")
    if response.objectIdentifier != objid or response.propertyIdentifier != prop:
        raise MismatchedResponse(
            f"asked {address} for {objid} log-buffer, got {response.objectIdentifier} {response.propertyIdentifier}"
        )
    records = response.itemData.cast_out(SequenceOf(LogRecord)) if response.itemCount else []
    out = []
    for record in records:
        when = from_bacnet_datetime(record.timestamp)
        if when is not None:
            out.append((when, record.logDatum))
    more = bool(response.resultFlags[2]) if response.resultFlags is not None else False
    return out, more


async def backfill(app, address, trend_log: str, object_type: str, since: float, until: float,
                   offset: dt.timedelta, max_records: int = 100_000, timeout: float | None = None) -> list[tuple[float, Any]]:
    """Every reading `trend_log` logged in (since, until) - unix seconds -
    as [(ts, value)], oldest first, at most `max_records`."""
    to_local = offset

    def local(ts: float) -> dt.datetime:
        return dt.datetime.fromtimestamp(ts, dt.timezone.utc).replace(tzinfo=None) + to_local

    def unix(local_time: dt.datetime) -> float:
        return (local_time - to_local).replace(tzinfo=dt.timezone.utc).timestamp()

    readings: list[tuple[float, Any]] = []
    cursor = local(since)
    end = local(until)
    while len(readings) < max_records:
        records, more = await read_records_after(app, address, trend_log, cursor, timeout=timeout)
        if not records:
            break
        for when, datum in records:
            if when >= end:
                return readings
            value = datum_value(datum, object_type)
            if value is not None and when > local(since):
                readings.append((unix(when), value))
        newest = records[-1][0]
        if not more or newest <= cursor:  # no progress: stop rather than loop on a device's bad answer
            break
        cursor = newest
    return readings[:max_records]
