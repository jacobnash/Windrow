"""
Tag export helpers for hszinc/pyhaystack grids - used by
haxall_mqtt_bridge.py, whose Haxall demo tooling still talks to Haxall
through pyhaystack (haxall_client.py). Everything else reads Haystack
servers through openhay (see haystack_source.py, whose own extract_tags
handles openhay values).
"""

import datetime

import hszinc

# Operational/live-state fields, not semantic tags - either the point's
# identity (id), Haxall bookkeeping (mod), or live cur/write state that's
# already carried on the value topic and would be instantly stale if
# republished as a "static" tag captured once at startup.
EXCLUDED_FIELDS = {
    "id", "mod",
    "curVal", "curStatus", "curErr",
    "writeVal", "writeLevel", "writeErr",
    "hisStatus", "hisErr",
}



def to_jsonable(value):
    """Every real Haystack v3/v4 value kind -> a JSON-safe representation.
    A deny-list of excluded fields plus this converter (rather than an
    allow-list of markers we happened to think of) is what makes tag
    export survive real building data instead of just our own demo
    fixtures - any marker or valued tag on the rec gets carried, whatever
    it's called. Falls back to str() for anything unrecognized (a future
    defs version, a vendor extension) so export never crashes on a kind
    this code doesn't know about yet - degraded fidelity beats a dropped
    point or a dead bridge process."""
    if value is hszinc.MARKER:
        return True
    if value is hszinc.NA:
        return "na"
    if value is hszinc.REMOVE:
        return "remove"
    if isinstance(value, hszinc.Ref):
        return value.value or value.name
    if isinstance(value, hszinc.Uri):
        return str(value)
    if isinstance(value, hszinc.XStr):
        return {"encoding": value.encoding, "data": value.data}
    if isinstance(value, hszinc.Coordinate):
        return {"lat": value.latitude, "lng": value.longitude}
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if hasattr(value, "magnitude") and hasattr(value, "units"):  # pint Quantity, if pint is ever installed
        return {"val": float(value.magnitude), "unit": str(value.units)}
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def extract_tags(row) -> dict:
    """Every real tag on the rec except the excluded operational fields -
    read dynamically, not off a hardcoded per-point list, so this stays
    correct for any Haystack v3/v4 tag combination without a code edit.

    `readAll()` over multiple points returns one grid whose columns are the
    UNION of every column across all rows - a row is padded with Python
    `None` for any column it doesn't actually have (confirmed live: a
    7-point grid gave a temp sensor row `zone: None`, `damper: None`, etc.
    for tags that belong to *other* rows in the same grid). None here means
    "column doesn't apply to this row," not "tag present with no value" -
    a real absent/removed value is hszinc.NA/hszinc.REMOVE, not None."""
    return {
        tag: to_jsonable(value)
        for tag, value in row.items()
        if tag not in EXCLUDED_FIELDS and value is not None
    }
