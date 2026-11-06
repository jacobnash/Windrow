"""
Any Project Haystack server as a Windrow source: a Niagara station running
nhaystack, SkySpark, Haxall, or another Baler. Points arrive in Baler over
the same MQTT envelope BACnet and Modbus use, so nothing downstream changes.

    python -m windrow.haystack_source --url https://station/haystack \\
        --user admin --password ... --source-id niagara-hq \\
        --mqtt-host broker --site-id hq --instance-id station-1

- **Discovery:** `read` the points matching `--filter`, then their
  equipment. Each equip becomes topic prefix `fbf/<source-id>/<equip-slug>`
  (unassigned points go under `.../misc`), and each point a stable slug
  label. Tags go out on `{prefix}/{label}/tags` and `{prefix}/equip/tags`,
  as for BACnet connections.
- **Live values:** `watchSub` + `watchPoll` (resubscribing when the server
  drops the watch); servers without watches fall back to polling `read`.
  Only changed values are published.
- **History:** optional `--backfill <range>` replays each `his` point's
  `hisRead` once on start, with the original timestamps.
- **Writes:** with `--site-id`, the command channel's `point_write` action
  (see docs/command-channel.md) becomes a remote Haystack `pointWrite` at
  the same level, and equip tags advertise `windrowSite`/`windrowInstance`,
  so a Baler pointWrite reaches the station's point.

nhaystack specifics are handled generically: its `S.`/`H.` ids are just
ids, and Niagara-only tags (axType, axSlotPath, axHistoryId, ...) pass
through as ordinary string tags.
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from datetime import date, datetime
from datetime import time as dtime

import openhay as oh

from windrow import legacy, mqtt_sink, tracing

tracer = tracing.get_tracer(__name__)

# Live/bookkeeping fields: carried on the value topic or meaningless once
# copied (ids and refs are re-derived from topics on the Baler side).
EXCLUDED_TAGS = {
    "id", "mod", "equipRef", "siteRef", "spaceRef",
    "curVal", "curStatus", "curErr", "writeVal", "writeLevel", "writeErr", "hisStatus", "hisErr",
}


class SourceError(ValueError):
    pass


def slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9_-]+", "-", str(text).strip().lower()).strip("-")
    return s or "unnamed"


def to_jsonable(v):
    """Haystack value (openhay types) -> JSON for the MQTT envelope."""
    if v is oh.MARKER:
        return True
    if v is oh.NA:
        return "na"
    if v is oh.REMOVE or v is None:
        return None
    if isinstance(v, oh.Number):
        return v.val
    if isinstance(v, oh.Ref):
        return v.id
    if isinstance(v, (oh.Uri, oh.Symbol)):
        return v.val
    if isinstance(v, oh.XStr):
        return {"type": v.type, "val": v.val}
    if isinstance(v, oh.Coord):
        return {"lat": v.lat, "lng": v.lng}
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: to_jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [to_jsonable(x) for x in v]
    if isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


def extract_tags(rec: dict) -> dict:
    return {k: to_jsonable(v) for k, v in rec.items() if k not in EXCLUDED_TAGS and v is not None}


def _name(rec: dict) -> str:
    for key in ("navName", "dis"):
        if isinstance(rec.get(key), str) and rec[key].strip():
            return rec[key]
    rid = rec["id"].id if isinstance(rec.get("id"), oh.Ref) else str(rec.get("id", "unnamed"))
    return rid.rsplit(".", 1)[-1]


class HaystackSource:
    def __init__(self, client: oh.Client, mqtt_client, source_id: str, filter: str = "point",
                 site_id: str | None = None, instance_id: str | None = None):
        self.client = client
        self.mqtt = mqtt_client
        self.source_id = slug(source_id)
        self.filter = filter
        self.site_id = site_id
        self.instance_id = instance_id
        self._lock = threading.Lock()  # one remote call at a time (poll loop vs command thread)
        self.points: dict[str, dict] = {}  # ref id -> {"prefix","label","rec"}
        self.by_topic: dict[str, str] = {}  # "prefix/label" -> ref id
        self.equips: dict[str, tuple[str, dict]] = {}  # prefix -> (equip id, rec)
        self._last: dict[str, object] = {}
        self._watch_id: str | None = None
        self._watch_supported = True

    # ------------------------------------------------------------ discovery

    def discover(self) -> int:
        with tracer.start_as_current_span("haystack_source.discover") as span, self._lock:
            recs = [r for r in self.client.read(self.filter) if isinstance(r.get("id"), oh.Ref)]
            equip_ids = sorted({r["equipRef"].id for r in recs if isinstance(r.get("equipRef"), oh.Ref)})
            equips = dict(zip(equip_ids, self.client.read_ids(equip_ids))) if equip_ids else {}
        self.points.clear()
        self.by_topic.clear()
        self.equips.clear()
        self._watch_id = None  # resubscribe with the new point list on the next poll
        taken: dict[str, set[str]] = {}
        for rec in sorted(recs, key=lambda r: r["id"].id):
            eref = rec.get("equipRef")
            equip = equips.get(eref.id) if isinstance(eref, oh.Ref) else None
            prefix = f"fbf/{self.source_id}/{slug(_name(equip)) if equip else 'misc'}"
            if equip:
                self.equips.setdefault(prefix, (eref.id, equip))
            label = slug(_name(rec))
            used = taken.setdefault(prefix, set())
            base, n = label, 2
            while label in used:  # two points with the same name on one equip
                label, n = f"{base}-{n}", n + 1
            used.add(label)
            self.points[rec["id"].id] = {"prefix": prefix, "label": label, "rec": rec}
            self.by_topic[f"{prefix}/{label}"] = rec["id"].id
        span.set_attribute("points", len(self.points))
        return len(self.points)

    def publish_tags(self) -> None:
        by_prefix: dict[str, list[str]] = {}
        for rid, p in self.points.items():
            tags = extract_tags(p["rec"])
            tags["sourceId"] = rid
            mqtt_sink.publish_reading(self.mqtt, p["prefix"], f"{p['label']}/tags", json.dumps(tags))
            by_prefix.setdefault(p["prefix"], []).append(p["label"])
        for prefix, labels in by_prefix.items():
            equip = self.equips.get(prefix)
            tags = extract_tags(equip[1]) if equip else {}
            if equip:
                tags["sourceId"] = equip[0]
            if self.site_id:
                tags.update(windrowSite=self.site_id, windrowInstance=self.instance_id)
            mqtt_sink.publish_reading(self.mqtt, prefix, "equip/tags", json.dumps({"tags": tags, "points": sorted(labels)}))

    # --------------------------------------------------------------- values

    def _publish_value(self, rec: dict, force: bool = False) -> bool:
        rid = rec["id"].id if isinstance(rec.get("id"), oh.Ref) else None
        if rid not in self.points or "curVal" not in rec:
            return False
        value = to_jsonable(rec["curVal"])
        if not force and self._last.get(rid) == value:
            return False
        self._last[rid] = value
        p = self.points[rid]
        mqtt_sink.publish_reading(self.mqtt, p["prefix"], p["label"], value)
        return True

    def poll_once(self, lease_secs: float = 300.0) -> int:
        """Publishes changed values; returns how many."""
        refs = list(self.points)
        if not refs:
            return 0
        with self._lock:
            if self._watch_supported:
                try:
                    if self._watch_id is None:
                        self._watch_id, rows = self.client.watch_sub(f"windrow {self.source_id}", refs, lease_secs)
                        return sum(self._publish_value(r, force=True) for r in rows)
                    rows = self.client.watch_poll(self._watch_id)
                except oh.HaystackError as exc:
                    if self._watch_id is None:  # watchSub itself unsupported: poll with read
                        self._watch_supported = False
                        print(f"haystack_source: no watches ({exc}); polling with read")
                    else:  # watch expired/unknown on the server: resubscribe next time
                        self._watch_id = None
                    rows = [r for r in self.client.read_ids(refs) if r]
            else:
                rows = [r for r in self.client.read_ids(refs) if r]
        return sum(self._publish_value(r) for r in rows)

    def backfill(self, range_: str) -> int:
        n = 0
        for rid, p in self.points.items():
            if "his" not in p["rec"]:
                continue
            with self._lock:
                try:
                    grid = self.client.his_read(oh.Ref(rid), range_)
                except oh.HaystackError as exc:
                    print(f"haystack_source: hisRead {rid} failed: {exc}")
                    continue
            for row in grid.rows:
                ts, val = row.get("ts"), row.get("val")
                if isinstance(ts, datetime) and val is not None:
                    mqtt_sink.publish_reading(self.mqtt, p["prefix"], p["label"], to_jsonable(val), ts=ts.timestamp())
                    n += 1
        return n

    # --------------------------------------------------------------- writes

    def write_point(self, topic: str, value, priority: int, expires_at: float | None = None) -> dict:
        if expires_at is not None and time.time() > expires_at:
            raise SourceError("command expired before it reached the site; not applied")
        if not isinstance(priority, int) or isinstance(priority, bool) or not 1 <= priority <= 17:
            raise SourceError(f"priority must be 1-17, got {priority!r}")
        rid = self.by_topic.get(topic)
        if rid is None:
            raise SourceError(f"no Haystack point publishes {topic!r}")
        rec = self.points[rid]["rec"]
        if "writable" not in rec:
            raise SourceError(f"{topic} ({rid}) is not writable on the source server")
        val = value if value is None or isinstance(value, (bool, str)) else oh.Number(float(value))
        with self._lock:
            self.client.point_write(oh.Ref(rid), priority, val, who=f"windrow:{self.source_id}")
            fresh = self.client.read_ids([oh.Ref(rid)])[0] or {}
        present = to_jsonable(fresh.get("curVal"))
        if "curVal" in fresh:
            self._publish_value(fresh, force=True)
        return {"topic": topic, "priority": priority, "value": value, "present_value": present}

    def on_command(self, client, userdata, msg) -> None:
        """Same contract as windrow.api's command channel: result (with the
        command's id/action echoed) on cmd/<site>/<instance>/result."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            action = payload["action"]
        except (ValueError, KeyError, UnicodeDecodeError) as exc:
            print(f"malformed command on {msg.topic}: {exc!r}")
            return
        try:
            if action != "point_write":
                raise SourceError(f"action {action!r} isn't supported by a Haystack source")
            result = self.write_point(payload["topic"], payload.get("value"), payload["priority"], payload.get("expires_at"))
            outcome = {"ok": True, "result": result}
        except Exception as exc:  # noqa: BLE001 - one bad command must not crash the mqtt thread
            outcome = {"ok": False, "error": str(exc)}
        outcome["action"] = action
        if payload.get("id") is not None:
            outcome["id"] = payload["id"]
        client.publish(f"cmd/{self.site_id}/{self.instance_id}/result", json.dumps(outcome), qos=1)


def main() -> None:
    p = argparse.ArgumentParser(description="Bridge a Haystack server (nhaystack, SkySpark, Haxall) into Windrow's MQTT feed")
    p.add_argument("--url", required=True, help="API root, e.g. https://station/haystack or http://host/api/demo")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--bearer", help="OAuth/API-key token instead of SCRAM user/password")
    p.add_argument("--format", default="zinc", choices=["zinc", "json3", "hayson"])
    p.add_argument("--source-id", required=True, help="names this source in topics: fbf/<source-id>/...")
    p.add_argument("--filter", default="point")
    p.add_argument("--interval", type=float, default=5.0, help="seconds between watch polls")
    p.add_argument("--rediscover", type=float, default=3600.0, help="seconds between re-reading the point list")
    p.add_argument("--backfill", default=None, help="hisRead range to replay once on start, e.g. yesterday")
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--mqtt-username", default=None)
    p.add_argument("--mqtt-password", default=None)
    p.add_argument("--site-id", default=legacy.env("WINDROW_SITE_ID"))
    p.add_argument("--instance-id", default=legacy.env("WINDROW_INSTANCE_ID"))
    args = p.parse_args()

    tracing.init_tracing("windrow-haystack-source")
    client = oh.Client(args.url, bearer=args.bearer, format=args.format)
    if not args.bearer:
        client.login(args.user, args.password)

    instance = args.instance_id or f"haystack-{slug(args.source_id)}"
    source = None
    command_topic = f"cmd/{args.site_id}/{instance}/#" if args.site_id else None

    def _resubscribe(c, userdata, flags, reason_code, properties):
        c.subscribe(command_topic, qos=1)

    mqtt_client = mqtt_sink.connect(
        args.mqtt_host, args.mqtt_port,
        client_id=f"windrow-hs-{args.site_id}-{instance}" if args.site_id else None,
        clean_session=False if args.site_id else None,
        username=args.mqtt_username, password=args.mqtt_password,
        on_connect_extra=_resubscribe if command_topic else None,
    )
    source = HaystackSource(client, mqtt_client, args.source_id, args.filter, args.site_id, instance if args.site_id else None)
    if command_topic:
        mqtt_client.message_callback_add(command_topic, source.on_command)

    print(f"haystack_source: {source.discover()} points from {args.url}")
    source.publish_tags()
    if args.backfill:
        print(f"haystack_source: backfilled {source.backfill(args.backfill)} samples")
    last_discovery = time.monotonic()
    while True:
        try:
            source.poll_once()
        except oh.AuthError:
            client.login(args.user, args.password) if not args.bearer else None
        except oh.HaystackError as exc:
            print(f"haystack_source: poll failed: {exc}")
        if time.monotonic() - last_discovery > args.rediscover:
            source.discover()
            source.publish_tags()
            last_discovery = time.monotonic()
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
