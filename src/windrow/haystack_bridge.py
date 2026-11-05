#!/usr/bin/env python3
"""
Bidirectional Haystack HTTP API bridge - the connector a Haystack-speaking
source (Haxall or SkySpark) gets bridged through. Read mode polls the
Phase 2 tagging model and republishes through the exact same mqtt_sink
envelope every other bridge uses, so Baler's existing fbf/#
listener needs zero changes to pick it up. Write mode proves round-trip
control against the dedicated "Windrow Write Demo" fixture point (never the
live 4 points, which race a real 5-second ingestion task) - see
docs/haystack-tagging-model.md.
"""

import argparse
import datetime
import json
import time

import hszinc

from windrow import mqtt_sink, tracing
from windrow.haxall_client import connect, eval_axon, point_write
from windrow.provision_haxall import find_ref

tracer = tracing.get_tracer(__name__)

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


def slug(dis: str) -> str:
    return dis.lower().replace(" ", "-")


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


def read_once(session, client, topic_prefix: str, publish_tags: bool) -> int:
    """One poll cycle over the 4 tagged points + the write-demo fixture.
    Returns the error count - callers decide whether that's fatal."""
    with tracer.start_as_current_span("haystack_bridge.read_cycle") as span:
        rows = eval_axon(session, "readAll(point and (equipRef or writable))")
        span.set_attribute("point_count", len(rows))
        errors = 0
        for row in rows:
            label = slug(row["dis"])
            try:
                mqtt_sink.publish_reading(client, topic_prefix, label, to_jsonable(row.get("curVal")))
                if publish_tags:
                    tags = extract_tags(row)
                    mqtt_sink.publish_reading(client, topic_prefix, f"{label}/tags", json.dumps(tags))
            except Exception as exc:
                errors += 1
                span.set_attribute(f"error.{label}", str(exc))
        span.set_attribute("error_count", errors)
        return errors


def publish_equip_tags(session, client, topic_prefix: str) -> int:
    """readAll(equip or site) once, publishing each rec's own tags to
    {topic_prefix}/equip/{haystackRef}/tags - the equip-side half of
    hasPoint/isPointOf linking (see docs/haystack-tagging-model.md and
    baler's ingest.link_equip_ref, which resolves a point's
    equipRef against exactly this identity). Points don't need re-reading
    here: each point's own equipRef tag (already carried losslessly by
    read_once's extract_tags -> Baler's HAYSTACK:equipRef triple)
    is what does the resolving, on the Baler side, once both
    messages have landed - so publish order between this and read_once
    doesn't matter."""
    with tracer.start_as_current_span("haystack_bridge.equip_tags_cycle") as span:
        rows = eval_axon(session, "readAll(equip or site)")
        span.set_attribute("equip_count", len(rows))
        errors = 0
        for row in rows:
            ref = to_jsonable(row["id"])
            try:
                tags = extract_tags(row)
                mqtt_sink.publish_reading(client, topic_prefix, f"equip/{ref}/tags", json.dumps(tags))
            except Exception as exc:
                errors += 1
                span.set_attribute(f"error.{ref}", str(exc))
        span.set_attribute("error_count", errors)
        return errors


def run_read_mode(session, client, topic_prefix: str, interval: float, once: bool) -> None:
    tags_sent = False
    while True:
        read_once(session, client, topic_prefix, publish_tags=not tags_sent)
        if not tags_sent:
            publish_equip_tags(session, client, topic_prefix)
        tags_sent = True
        if once:
            return
        time.sleep(interval)


def run_write_mode(session, client, topic_prefix: str, value: float) -> float | None:
    with tracer.start_as_current_span("haystack_bridge.write") as span:
        ref = find_ref(session, 'point and dis=="Windrow Write Demo"')
        span.set_attribute("point_ref", ref)
        span.set_attribute("value", value)

        point_write(session, ref, value)
        row = eval_axon(session, f"readById(@{ref})")[0]
        readback = to_jsonable(row.get("writeVal"))

        span.set_attribute("success", readback == value)
        mqtt_sink.publish_reading(client, topic_prefix, slug("Windrow Write Demo"), readback)
        return readback


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--haxall-uri", default="http://localhost:8180/")
    parser.add_argument("--haxall-user", default="su")
    parser.add_argument("--haxall-pass", default="windrow-demo-pass")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--topic-prefix", default="fbf/haxall-readback")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true", help="read mode: one cycle then exit")
    parser.add_argument("--write-value", type=float, default=None, help="write mode: one-shot write to the Windrow Write Demo point, then exit")
    args = parser.parse_args()

    tracing.init_tracing("windrow-haystack-bridge")
    session = connect(args.haxall_uri, args.haxall_user, args.haxall_pass)
    client = mqtt_sink.connect(args.mqtt_host, args.mqtt_port)

    if args.write_value is not None:
        readback = run_write_mode(session, client, args.topic_prefix, args.write_value)
        print(f"wrote {args.write_value}, read back {readback}")
    else:
        run_read_mode(session, client, args.topic_prefix, args.interval, args.once)


if __name__ == "__main__":
    main()
