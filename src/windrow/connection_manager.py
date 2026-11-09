"""
Owns the one shared bacpypes3 Application and every active connection's
poll task - the dynamic counterpart to bridge.py's static, CLI-args-only
poll loop. Every public method here is meant to be called via
asyncio.run_coroutine_threadsafe() from api.py's HTTP handler thread, so
every method (reads included - see api.py's module docstring for why) is
async and touches only loop-thread-owned state.
"""

import asyncio
import contextlib
import json
import math
import os
import random
import time
from collections import Counter

from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.basetypes import ErrorType

from windrow import (
    bacnet_alarms,
    bacnet_batch,
    bacnet_cov,
    bacnet_network,
    bacnet_schedules,
    bacnet_tagger,
    bacnet_trend,
    connection_store,
    discovery,
    mqtt_sink,
    tracing,
)
from windrow.bacnet_read import (
    READ_TIMEOUT,
    RPMUnsupported,
    Sample,
    read_present_value,
    read_present_values,
)
from windrow.bacnet_values import normalize_value, priority_levels
from windrow.poll_status import PollStatus

tracer = tracing.get_tracer(__name__)

_TAGGER_RULES = bacnet_tagger.load_default_rules()


def should_publish(
    value,
    last_value,
    last_published_at: float | None,
    now: float,
    his_collect_cov: bool | float | None = None,
    his_collect_cov_rate_limit: float | None = None,
    his_collect_interval: float | None = None,
) -> bool:
    """Mirrors Haxall's real hisCollectCov/hisCollectCovRateLimit/
    hisCollectInterval semantics - verified against the actual product
    (a decompiled-free look at hxPoint.pod's bundled def docs), not
    guessed. A point configured with neither always publishes - today's
    default behavior, unchanged, so this stays opt-in and backward
    compatible.

    his_collect_cov: True means collect on any change (Haxall's marker
    form); a number means a numeric tolerance the value must move by
    before it's logged (non-numeric values always compare by equality
    regardless of this being a number - matches Haxall's own fallback).

    Deliberately simpler than Haxall's own tiered COV rate-limit
    fallback (1/10 of hisCollectInterval or 1min, else 1min numeric /
    1sec non-numeric) - no default limit unless
    his_collect_cov_rate_limit is set explicitly.
    ponytail: no default rate-limit tiering, add Haxall's fallback tiers
    if noisy-point throttling becomes a real problem without one.
    """
    if his_collect_cov is None and his_collect_interval is None:
        return True

    if last_published_at is None:
        return True

    elapsed = now - last_published_at

    if his_collect_cov is None:
        return elapsed >= his_collect_interval

    numeric_cov = isinstance(his_collect_cov, (int, float)) and not isinstance(his_collect_cov, bool)
    both_numeric = (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isinstance(last_value, (int, float))
        and not isinstance(last_value, bool)
    )
    changed = abs(value - last_value) >= his_collect_cov if (numeric_cov and both_numeric) else value != last_value

    if not changed:
        return False
    if his_collect_cov_rate_limit is None:
        return True
    return elapsed >= his_collect_cov_rate_limit


# BACnet object types whose present-value is commandable (has a 16-level
# priority array). Points of these types are tagged `writable`, which is
# what lets Baler's Haystack pointWrite target them.
COMMANDABLE_TYPES = {
    "analog-output", "analog-value", "binary-output", "binary-value",
    "multi-state-output", "multi-state-value",
}


# What a stored BACnet point keeps. Anything else a caller sends (e.g. the
# present_value snapshot /learn returns) is dropped on update rather than
# frozen into the state file as if it meant something.
POINT_FIELDS = {
    "object_identifier", "label", "units", "description", "poll_interval",
    "his_collect_cov", "his_collect_cov_rate_limit", "his_collect_interval",
    "subscribe_cov", "trend_log",
    # raw BACnet metadata from /learn (Windrow#19), published with the tags
    "object_type", "commandable", "state_text", "active_text", "inactive_text",
    "min_pres_value", "max_pres_value", "cov_increment", "structured_views", "device",
}
# How a connection reads its device (Baler#127). All optional; a record
# without them reads with ReadPropertyMultiple and doesn't subscribe.
#   read_multiple      batch this connection's reads into RPMs (default true)
#   read_multiple_max  most objects per RPM to this device (default: what
#                      the device's max APDU fits, at most WINDROW_RPM_MAX_POINTS)
#   subscribe_cov      SubscribeCOV for every point (a point's own
#                      subscribe_cov overrides it), polling slowly underneath
#   cov_lifetime       seconds each subscription asks for; renewed at half
#   cov_poll_interval  the slow fallback poll for a subscribed point
#   backfill_window    trend-log backfill (Baler#129, below) reaches back at
#                      most this many seconds; 0 turns it off
#   forward_alarms     register as a recipient in the device's notification
#                      classes and publish its alarms (Baler#164) - off by
#                      default: it changes the controller's configuration
#   priority_interval  seconds between reads of every commandable point's
#                      priority-array, published as the reading's
#                      `priority` (Windrow#25); 0 turns it off
#   read_quality       read each point's status-flags and reliability with
#                      its present-value and publish them as the reading's
#                      `quality` (Windrow#20) - on by default; batched reads
#                      and COV notifications only, a single ReadProperty
#                      reads the value alone
READ_OPTION_FIELDS = {
    "read_multiple", "read_multiple_max", "subscribe_cov", "cov_lifetime", "cov_poll_interval", "backfill_window",
    "forward_alarms", "priority_interval", "read_quality",
}
UPDATABLE_FIELDS = {"points", "poll_interval", "topic_prefix", "device_address", "device_name"} | READ_OPTION_FIELDS
DEFAULT_COV_LIFETIME = 300
# Priority arrays (Windrow#25): read every minute by default - an override
# shows up within a minute, at one RPM per PRIORITY_BATCH commandable points.
DEFAULT_PRIORITY_INTERVAL = float(os.environ.get("WINDROW_PRIORITY_INTERVAL", "60"))
# Objects per priority-array RPM: a full array of Reals is ~100 bytes.
PRIORITY_BATCH = 8
DEFAULT_COV_POLL_INTERVAL = 300.0
# How often a forward_alarms connection re-checks its recipient entries (a
# controller restored from a backup loses them) and re-reads active alarms.
ALARM_REFRESH_INTERVAL = float(os.environ.get("WINDROW_ALARM_REFRESH", "3600"))
# Trend-log backfill (Baler#129): a point with `trend_log` (the TrendLog
# object logging it on the controller, e.g. "trend-log,3") gets the
# readings it missed read back from that log - after Windrow was down
# (from the last reading it took, remembered in a marks file next to the
# state file) and after the device was unreachable - going back at most
# `backfill_window` seconds.
DEFAULT_BACKFILL_WINDOW = 86400.0
# How often the marks file is rewritten while readings come in.
MARKS_SAVE_INTERVAL = 60.0


def check_backfill_window(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"backfill_window must be a number of seconds >= 0, got {value!r}")
    return value


def validate_read_options(options: dict) -> dict:
    """The READ_OPTION_FIELDS present in `options`, type-checked. ValueError
    (a 400) for a wrong type, same as every other connection field."""
    cleaned = {}
    for key in ("read_multiple", "subscribe_cov", "forward_alarms", "read_quality"):
        if key in options:
            if not isinstance(options[key], bool):
                raise ValueError(f"{key} must be true or false, got {options[key]!r}")
            cleaned[key] = options[key]
    if "read_multiple_max" in options:
        cleaned["read_multiple_max"] = connection_store.check_int("read_multiple_max", options["read_multiple_max"], 1, 500)
    if "cov_lifetime" in options:
        # 0 would mean "forever" to the device, which outlives us; keep it renewable
        cleaned["cov_lifetime"] = connection_store.check_int("cov_lifetime", options["cov_lifetime"], 10, 86400)
    if "cov_poll_interval" in options:
        cleaned["cov_poll_interval"] = connection_store.check_number("cov_poll_interval", options["cov_poll_interval"])
    if "backfill_window" in options:
        cleaned["backfill_window"] = check_backfill_window(options["backfill_window"])
    if "priority_interval" in options:
        value = options["priority_interval"]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0 or 0 < value < 5:
            raise ValueError(f"priority_interval must be 0 (off) or a number of seconds >= 5, got {value!r}")
        cleaned["priority_interval"] = value
    return cleaned


def validate_points(points) -> list[dict]:
    """A non-empty list of {object_identifier, label, ...} with unique
    labels (two points on one label would publish to the same topic)."""
    if not isinstance(points, list) or not points:
        raise ValueError("points must be a non-empty list of {object_identifier, label} objects")
    cleaned, labels = [], set()
    for point in points:
        if not isinstance(point, dict):
            raise ValueError(f"each point must be an object, got {point!r}")  # noqa: TRY004 - ValueError means 400 / ok:false
        connection_store.check_text("object_identifier", point.get("object_identifier"))
        label = connection_store.check_text("label", point.get("label"))
        if label in labels:
            raise ValueError(f"duplicate point label {label!r}")
        labels.add(label)
        if "poll_interval" in point:
            connection_store.check_number("point poll_interval", point["poll_interval"])
        if "trend_log" in point:
            connection_store.check_text("trend_log", point["trend_log"])
        if "subscribe_cov" in point and not isinstance(point["subscribe_cov"], bool):
            raise ValueError(f"subscribe_cov must be true or false, got {point['subscribe_cov']!r}")
        _check_learned_metadata(point)
        cov = point.get("his_collect_cov")
        if cov is not None and not isinstance(cov, (bool, int, float)):
            raise ValueError(f"his_collect_cov must be true/false or a number, got {cov!r}")
        for key in ("his_collect_cov_rate_limit", "his_collect_interval"):
            if point.get(key) is not None:
                connection_store.check_number(key, point[key])
        cleaned.append({k: v for k, v in point.items() if k in POINT_FIELDS})
    return cleaned


def _check_learned_metadata(point: dict) -> None:
    """The /learn metadata fields are stored and republished as given, so
    only their shapes are checked: a wrong type is a 400, not a tag nobody
    can read."""
    for key in ("object_type", "active_text", "inactive_text"):
        if point.get(key) is not None and not isinstance(point[key], str):
            raise ValueError(f"{key} must be a string, got {point[key]!r}")
    for key in ("min_pres_value", "max_pres_value", "cov_increment"):
        value = point.get(key)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
            raise ValueError(f"{key} must be a number, got {value!r}")
    if point.get("commandable") is not None and not isinstance(point["commandable"], bool):
        raise ValueError(f"commandable must be true or false, got {point['commandable']!r}")
    for key in ("state_text", "structured_views"):
        value = point.get(key)
        if value is not None and not (isinstance(value, list) and all(isinstance(v, str) for v in value)):
            raise ValueError(f"{key} must be a list of strings, got {value!r}")
    device = point.get("device")
    if device is not None and not isinstance(device, dict):
        raise ValueError(f"device must be an object, got {device!r}")


async def stop_task(task: asyncio.Task | None) -> None:
    """Cancel and wait for it to actually finish, so nothing it was in the
    middle of (a read, a publish) lands after the caller moves on - e.g. a
    stale value republished to a topic update_connection just cleared."""
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task


class PointWriteError(ValueError):
    pass


class ReadFailed(RuntimeError):
    """The device answered a read with an error, reject, abort or nothing."""


class _PointState:
    """What one point's poll task and its COV subscription share: the last
    published value (so a notification and a poll apply the same hisCollect
    policy) and whether the subscription is live (which sets the poll rate)."""

    def __init__(self):
        self.last_value = None
        self.last_quality: dict | None = None
        self.last_published_at: float | None = None
        self.cov_active = False
        # the point's priority array as last read (bacnet_values.priority_levels)
        # and as last published, for commandable points (Windrow#25)
        self.priority: dict | None = None
        self.last_priority: dict | None = None
        self.wake = asyncio.Event()  # poll now: the subscription went away
        self.resubscribe = asyncio.Event()  # subscribe now: the device came back


# Module-level so tests that replace read_present_value(s) here are what the
# batchers call.
async def _read_one(app, address, objid):
    return await read_present_value(app, address, objid)


async def _read_many(app, address, objids, status=()):
    if status:
        return await read_present_values(app, address, objids, status=status)
    return await read_present_values(app, address, objids)


def _object_type(object_identifier: str) -> str:
    return str(object_identifier).split(",", 1)[0].strip()


# Reads in flight at once across every connection (Baler#47). bacpypes3
# runs on the same event loop as the HTTP API's calls; hundreds of
# outstanding reads - most of them to devices that no longer answer, each
# waiting out its APDU timeout and retrying at once - starved the loop
# until every API call timed out.
MAX_CONCURRENT_READS = int(os.environ.get("WINDROW_MAX_CONCURRENT_READS", "16"))
# Seconds a point write may take before it counts as failed (TimeoutError:
# a 504 over HTTP, ok:false on the command channel). Same reasoning as
# bacnet_read.READ_TIMEOUT.
WRITE_TIMEOUT = float(os.environ.get("WINDROW_WRITE_TIMEOUT", "10"))
# A point whose reads keep failing backs off up to this long between tries.
MAX_FAILURE_BACKOFF = 300.0
# After this many failures in a row to a routed (net:mac) device, forget the
# cached router for its network, so the next read looks for the router
# again - one that was replaced or re-addressed is otherwise asked forever.
ROUTE_RESET_FAILURES = 3


def failure_backoff(interval: float, consecutive_failures: int) -> float:
    """Seconds to wait before the next read: the point's interval while it
    answers, doubling per consecutive failure, capped at MAX_FAILURE_BACKOFF."""
    if consecutive_failures <= 0:
        return interval
    return min(MAX_FAILURE_BACKOFF, interval * (2 ** consecutive_failures))


class ConnectionManager:
    def __init__(
        self,
        app: Application,
        mqtt_client,
        state_file: str,
        site_id: str | None = None,
        instance_id: str | None = None,
    ):
        self.app = app
        self.mqtt_client = mqtt_client
        self.state_file = state_file
        # Set when the command channel is on: advertised on each
        # connection's equipment record as windrowSite/windrowInstance, so
        # Baler knows which cmd/<site>/<instance> topic owns a point.
        self.site_id = site_id
        self.instance_id = instance_id
        self.connections: dict[str, dict] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        # In-memory poll health per connection, never persisted (Baler#110).
        self.status = PollStatus()
        self._side_tasks: set[asyncio.Task] = set()
        self._read_slots: asyncio.Semaphore | None = None
        # one per device address, shared by every connection on it
        self.batchers: dict[str, bacnet_batch.ReadBatcher] = {}
        # each device's poll ticks sit at this fraction of the interval, so a
        # device's points tick together (one RPM) but devices don't all tick at once
        self._phase: dict[str, float] = {}
        # requests sent, by kind - how much the device is asked, not how many points
        self.stats: Counter = Counter()
        self._cov_live: set[int] = set()
        # per connection: set to re-read its priority arrays now (after a write)
        self._priority_wake: dict[str, asyncio.Event] = {}
        # Trend-backed points' last reading time (unix s), by
        # "<connection id>/<label>", so a restart knows where to backfill from.
        base, _ext = os.path.splitext(state_file)
        self.marks_file = f"{base}.trend-marks.json"
        self._marks: dict[str, float] = {}
        self._marks_saved_at = 0.0

    def _slots(self) -> asyncio.Semaphore:
        # created on first use, on the running loop (not in __init__, which
        # can run before the loop exists)
        if self._read_slots is None:
            self._read_slots = asyncio.Semaphore(MAX_CONCURRENT_READS)
        return self._read_slots

    async def start(self) -> None:
        """Restores persisted connections as running poll tasks - no fresh
        discovery needed, the device address is already known."""
        with tracer.start_as_current_span("connection_manager.start") as span:
            records = connection_store.load(self.state_file)
            self._marks = _load_marks(self.marks_file)
            for record in records:
                self.connections[record["id"]] = record
                self._start_poll_task(record)
                # Retained anyway, but republishing means connections made
                # before this release (or before --site-id was set) start
                # advertising their command route too - and their points
                # pick up tags added since (e.g. `unit`). Baler's tag ingest
                # is additive, so nothing a human added is lost.
                self._tag_points(record["topic_prefix"], record["points"])
                self._publish_equip_membership(record["topic_prefix"], record["points"])
            span.set_attribute("restored_count", len(records))

    async def discover(self, **kwargs) -> list[dict]:
        with tracer.start_as_current_span("connection_manager.discover"):
            return await discovery.discover_devices(self.app, **kwargs)

    async def learn(self, device_address: str, device_instance: int) -> list[dict]:
        with tracer.start_as_current_span("connection_manager.learn") as span:
            span.set_attribute("device_address", device_address)
            return await discovery.learn_points(self.app, device_address, device_instance)

    async def read_schedules(self, device_address: str, device_instance: int, labels: dict | None = None) -> dict:
        """The device's Schedule and Calendar objects, read now (Baler#165)."""
        with tracer.start_as_current_span("connection_manager.read_schedules") as span:
            span.set_attribute("device_address", device_address)
            bacnet_network.parse_device_address(device_address)
            async with self._slots():
                return await bacnet_schedules.read_device_schedules(self.app, device_address, device_instance, labels)

    async def connection_schedules(self, connection_id: str) -> dict | None:
        """read_schedules for a connection's device, with the objects its
        points poll named by their labels in each schedule's `controls`."""
        record = self.connections.get(connection_id)
        if record is None:
            return None
        labels = {str(_objid(p["object_identifier"])): p["label"] for p in record["points"]}
        return await self.read_schedules(record["device_address"], record["device_instance"], labels)

    async def create_connection(
        self,
        device_address: str,
        device_instance: int,
        topic_prefix: str,
        points: list[dict],
        poll_interval: float = 5.0,
        credential: dict | None = None,
        device_name: str | None = None,
        read_options: dict | None = None,
    ) -> dict:
        with tracer.start_as_current_span("connection_manager.create_connection") as span:
            bacnet_network.parse_device_address(device_address)  # ValueError (a 400) for a broadcast or a typo
            device_name = connection_store.check_device_name(device_name)
            record = {
                "id": connection_store.new_connection_id(),
                "device_address": device_address,
                "device_instance": device_instance,
                "topic_prefix": topic_prefix,
                "poll_interval": poll_interval,
                "points": points,
                "credential": credential,
                **validate_read_options(read_options or {}),
            }
            if device_name:
                record["device_name"] = device_name
            self.connections[record["id"]] = record
            self._apply_batch_limit(device_address)
            self._save()
            self._tag_points(topic_prefix, points)
            self._publish_equip_membership(topic_prefix, points)
            self._start_poll_task(record)
            span.set_attribute("connection_id", record["id"])
            span.set_attribute("point_count", len(points))
            return record

    def _publish_equip_membership(self, topic_prefix: str, points: list[dict]) -> None:
        """One message per connection, {prefix}/equip/tags - equipment
        identity is the connection's own topic_prefix (Baler's
        ingest.topic_prefix_to_equip_uri), deliberately not the BACnet
        device_instance: a human already draws the "this is one piece of
        equipment" boundary at connection-creation time by choosing which
        learned points go under this topic_prefix, and one physical device
        can expose more than one logical equipment unit (a chiller-plant
        controller owning several chillers) - reusing that existing
        decision is right more often than device_instance would be. No
        Brick-class tags are asserted here (no signal for that in raw
        BACnet driver metadata) - hasPoint structure is useful on its own,
        equipment classification is a separate, best-effort layer a human
        (or a future heuristic) can add on top via the same {prefix}/equip/tags
        topic, retained, at any time."""
        with tracer.start_as_current_span("connection_manager.publish_equip_membership") as span:
            span.set_attribute("point_count", len(points))
            tags = {}
            if self.site_id:
                tags = {"windrowSite": self.site_id, "windrowInstance": self.instance_id}
            payload = json.dumps({"tags": tags, "points": [p["label"] for p in points]})
            mqtt_sink.publish_reading(self.mqtt_client, topic_prefix, "equip/tags", payload)

    def _tag_points(self, topic_prefix: str, points: list[dict]) -> None:
        """Publishes {prefix}/{label}/tags once per point at connection
        creation, same wire convention haystack_source.py uses on
        the Haystack-source side - Baler's mqtt_listener ->
        ingest_tags -> mapping.classify_point needs no changes to pick
        these up, retained=True means a later windrow.api restart doesn't need
        to republish (connection_manager.start() just resumes polling,
        same as today). Auto-derived only from whatever driver metadata
        the caller included per point (label/object_identifier/description);
        a human can still tag/override on top through the exact same
        mechanism used for any other point."""
        with tracer.start_as_current_span("connection_manager.tag_points") as span:
            span.set_attribute("point_count", len(points))
            for point in points:
                tags = bacnet_tagger.tag_point(point, _TAGGER_RULES)
                # Commandable object, and not a sensor: BACnet will often
                # accept a write to a sensor-ish analog-value, but Haystack's
                # `writable` is for setpoints and commands.
                if _object_type(point.get("object_identifier", "")) in COMMANDABLE_TYPES and not tags.get("sensor"):
                    tags["writable"] = True
                # The BACnet units the point was learned with, as Haystack's
                # `unit` tag - Baler shows curVal/history with it, and unit
                # filters (`temp > 70°F`) need it. An explicit Haystack
                # `unit` on the point wins, for a device that reports none
                # or the wrong one (Baler#82).
                unit = point.get("unit") or bacnet_tagger.haystack_unit(point.get("units"))
                tag_payload: dict = dict(tags)
                # the raw evidence /learn captured, for Baler to judge (Windrow#19)
                for tag, value in bacnet_tagger.raw_metadata_tags(point).items():
                    tag_payload.setdefault(tag, value)
                if unit:
                    tag_payload["unit"] = unit
                mqtt_sink.publish_reading(self.mqtt_client, topic_prefix, f"{point['label']}/tags", json.dumps(tag_payload))

    def _find_point(self, topic: str) -> tuple[dict, dict]:
        for record in self.connections.values():
            prefix = record["topic_prefix"].rstrip("/")
            if topic.startswith(prefix + "/"):
                label = topic[len(prefix) + 1:]
                for point in record["points"]:
                    if point["label"] == label:
                        return record, point
        raise PointWriteError(f"no connected point publishes {topic!r}")

    async def write_point(self, topic: str, value, priority: int, expires_at: float | None = None) -> dict:
        """BACnet WriteProperty(present-value) at `priority` (1-16, or 17 below); a None
        value relinquishes that level. Priority 17 is Haystack's "default"
        level: it writes the object's relinquish-default (the value used when
        every level is empty), which can be set but not relinquished. Reads the value back and publishes it
        straight away, so Baler's curVal reflects the command without
        waiting for the next poll. `expires_at` (unix seconds) protects
        against a command that sat in the broker while the site was offline
        being applied long after anyone asked for it."""
        with tracer.start_as_current_span("connection_manager.write_point") as span:
            span.set_attribute("topic", topic)
            span.set_attribute("priority", priority)
            if expires_at is not None and time.time() > expires_at:
                raise PointWriteError("command expired before it reached the site; not applied")
            if not isinstance(priority, int) or isinstance(priority, bool) or not 1 <= priority <= 17:
                raise PointWriteError(f"priority must be 1-17, got {priority!r}")
            if priority == 17 and value is None:
                raise PointWriteError("level 17 (relinquish-default) can be set but not relinquished")
            record, point = self._find_point(topic)
            obj_type = _object_type(point["object_identifier"])
            if obj_type not in COMMANDABLE_TYPES:
                raise PointWriteError(f"{topic} is a {obj_type}, which is not commandable")
            bacnet_value = _to_bacnet(value, obj_type)
            address, objid = record["device_address"], point["object_identifier"]
            # bacpypes3 raises a device's Error/Reject/Abort answer, and those
            # are BaseExceptions: uncaught, a refused write (write-access-
            # denied, value-out-of-range, unknown-object) got past the API's
            # `except Exception` - the HTTP client got no answer, and on the
            # command channel it escaped into paho's network thread. It also
            # had no timeout (Baler#84: a request to a device that stopped
            # answering never returns).
            try:
                if priority == 17:
                    write = self.app.write_property(address, objid, "relinquish-default", bacnet_value)
                else:
                    write = self.app.write_property(address, objid, "present-value", bacnet_value, priority=priority)
                await asyncio.wait_for(write, WRITE_TIMEOUT)
            except ErrorRejectAbortNack as answer:
                span.set_attribute("error", str(answer))
                raise PointWriteError(f"the device refused the write to {topic}: {answer}") from None
            # The read-back is a courtesy (curVal without waiting for the next
            # poll): if it fails, the write still happened - report it, publish
            # nothing, and let the poll catch up.
            try:
                raw = await asyncio.wait_for(self.app.read_property(address, objid, "present-value"), READ_TIMEOUT)
            except (Exception, ErrorRejectAbortNack) as exc:  # noqa: BLE001 - no read-back is not a failed write
                span.set_attribute("readback_error", repr(exc))
                raw = None
            present = None if raw is None or isinstance(raw, (ErrorRejectAbortNack, ErrorType)) else normalize_value(raw)
            if record.get("id") in self._priority_wake:
                self._priority_wake[record["id"]].set()  # the array just changed: show it
            if present is not None:
                mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], point["label"], present)
            return {"topic": topic, "priority": priority, "value": value, "present_value": present}

    async def delete_connection(self, connection_id: str) -> bool:
        with tracer.start_as_current_span("connection_manager.delete_connection") as span:
            span.set_attribute("connection_id", connection_id)
            if connection_id not in self.connections:
                span.set_attribute("found", False)
                return False

            task = self.tasks.pop(connection_id, None)
            if task is not None:
                task.cancel()
            removed = self.connections.pop(connection_id)
            if removed.get("forward_alarms"):
                self._background(self._unregister_alarms(removed))
            address = removed["device_address"]
            self._apply_batch_limit(address)
            self.status.reset(connection_id)
            self._save()
            # Clear the retained value/tags/alarm/equip topics this connection
            # published. Left on the broker, every listener restart replays
            # them and Baler re-creates the deleted points (Windrow #29).
            prefix = removed["topic_prefix"]
            for label in [p["label"] for p in removed["points"]]:
                for suffix in ("", "/tags", "/alarm"):
                    mqtt_sink.clear_retained(self.mqtt_client, prefix, f"{label}{suffix}")
            mqtt_sink.clear_retained(self.mqtt_client, prefix, "equip/tags")
            span.set_attribute("found", True)
            return True

    async def update_connection(self, connection_id: str, patch: dict) -> dict | None:
        """Edit points/poll_interval/topic_prefix in place: validate, persist,
        restart this connection's poll tasks, clear the retained value and
        tags of every point that no longer publishes where it did (all of
        them when the prefix moves - including the old equip record), then
        retag. The id survives, so the device registry's link to it does too."""
        with tracer.start_as_current_span("connection_manager.update_connection") as span:
            span.set_attribute("connection_id", connection_id)
            old = self.connections.get(connection_id)
            if old is None:
                span.set_attribute("found", False)
                return None
            connection_store.check_patch(patch, UPDATABLE_FIELDS)
            record = dict(old)
            if "points" in patch:
                record["points"] = validate_points(patch["points"])
            if "poll_interval" in patch:
                record["poll_interval"] = connection_store.check_number("poll_interval", patch["poll_interval"])
            if "topic_prefix" in patch:
                record["topic_prefix"] = connection_store.check_topic_prefix(patch["topic_prefix"])
            if "device_address" in patch:
                record["device_address"] = connection_store.check_text("device_address", patch["device_address"]).strip()
            if "device_name" in patch:
                name = connection_store.check_device_name(patch["device_name"])
                record.pop("device_name", None)
                if name:
                    record["device_name"] = name
            record.update(validate_read_options(patch))

            await stop_task(self.tasks.pop(connection_id, None))
            if old.get("forward_alarms") and (
                not record.get("forward_alarms") or record["device_address"] != old["device_address"]
            ):
                # off, or the device moved: the old address's entries go (the
                # restarted poll loop registers at the new one)
                self._background(self._unregister_alarms(old))
            self.status.reset(connection_id)
            self.connections[connection_id] = record
            self._apply_batch_limit(record["device_address"])
            self._save()

            old_labels = [p["label"] for p in old["points"]]
            if record["topic_prefix"] != old["topic_prefix"]:
                removed = old_labels
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], "equip/tags")
            else:
                kept = {p["label"] for p in record["points"]}
                removed = [label for label in old_labels if label not in kept]
            for label in removed:
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], label)
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], f"{label}/tags")

            self._tag_points(record["topic_prefix"], record["points"])
            self._publish_equip_membership(record["topic_prefix"], record["points"])
            self._start_poll_task(record)
            span.set_attribute("found", True)
            span.set_attribute("cleared_count", len(removed))
            return record

    async def list_connections(self) -> list[dict]:
        with tracer.start_as_current_span("connection_manager.list_connections") as span:
            connections = list(self.connections.values())
            span.set_attribute("count", len(connections))
            return connections

    async def set_credential(self, connection_id: str, credential: dict) -> dict | None:
        with tracer.start_as_current_span("connection_manager.set_credential") as span:
            span.set_attribute("connection_id", connection_id)
            record = self.connections.get(connection_id)
            if record is None:
                span.set_attribute("found", False)
                return None
            record["credential"] = credential
            self._save()
            span.set_attribute("found", True)
            return record

    def _save(self) -> None:
        connection_store.save(self.state_file, list(self.connections.values()))

    def reading_rate(self) -> float:
        """Readings per second across every point - what mqtt_sink sizes
        its outage spool by (the poll rate; an upper bound for a COV point)."""
        return sum(
            1.0 / point.get("poll_interval", record["poll_interval"])
            for record in self.connections.values() for point in record["points"]
        )

    def _mark(self, record: dict, point: dict, ts: float) -> None:
        if "trend_log" not in point:
            return
        self._marks[f"{record.get('id')}/{point['label']}"] = ts
        if time.monotonic() - self._marks_saved_at >= MARKS_SAVE_INTERVAL:
            self.save_marks()

    def save_marks(self) -> None:
        self._marks_saved_at = time.monotonic()
        try:
            _save_marks(self.marks_file, self._marks)
        except OSError as exc:  # a full disk mustn't stop polling; the next backfill just reaches further back
            print(f"couldn't save trend marks to {self.marks_file}: {exc!r}")

    async def _backfill(self, record: dict, point: dict, since: float, until: float) -> int:
        """Publish what the point's TrendLog recorded in (since, until), not
        retained and with each record's own timestamp; returns how many."""
        window = record.get("backfill_window", DEFAULT_BACKFILL_WINDOW)
        if not window:
            return 0
        since = max(since, until - window)
        if until - since < 1.0:
            return 0
        address = record["device_address"]
        try:
            async with self._slots():
                offset = await bacnet_trend.device_utc_offset(self.app, address, record["device_instance"])
            if offset is None:
                offset = bacnet_trend.host_utc_offset()
            async with self._slots():
                readings = await bacnet_trend.backfill(
                    self.app, address, point["trend_log"], _object_type(point["object_identifier"]), since, until, offset
                )
        except Exception as exc:  # noqa: BLE001 - a backfill that fails loses history, not the live poll
            print(f"backfill from {point['trend_log']} for {point['label']} failed: {exc!r}")
            return 0
        for ts, value in readings:
            mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], point["label"], value, ts=ts, retain=False)
        if readings:
            print(f"backfilled {len(readings)} reading(s) for {record['topic_prefix']}/{point['label']} "
                  f"from {point['trend_log']} ({until - since:.0f} s gap)")
        return len(readings)

    def read_stats(self) -> dict:
        """Requests sent so far by kind, and live COV subscriptions - the
        numbers that say whether batching and COV are doing their job."""
        return {**self.stats, "cov_subscriptions": len(self._cov_live)}

    def _device_max_apdu(self, address: str) -> int | None:
        """From the device's I-Am, if bacpypes3 has seen one."""
        try:
            from bacpypes3.pdu import Address

            info = self.app.device_info_cache.address_cache.get(Address(address))
        except Exception:  # noqa: BLE001 - no app (tests) or an address bacpypes3 can't parse: just the default cap
            return None
        return getattr(info, "max_apdu_length_accepted", None)

    def _batch_limit(self, address: str) -> int:
        status = any(
            record["device_address"] == address and record.get("read_quality", True)
            for record in self.connections.values()
        )
        limit = bacnet_batch.points_per_request(self._device_max_apdu(address), status=status)
        for record in self.connections.values():
            if record["device_address"] == address and "read_multiple_max" in record:
                limit = min(limit, record["read_multiple_max"])
        return limit

    def _apply_batch_limit(self, address: str) -> None:
        if address in self.batchers:
            self.batchers[address].max_points = self._batch_limit(address)

    def _batcher(self, address: str) -> bacnet_batch.ReadBatcher:
        batcher = self.batchers.get(address)
        if batcher is None:
            batcher = bacnet_batch.ReadBatcher(
                self.app, address, self._slots, _read_one, _read_many, self.stats,
                max_points=self._batch_limit(address),
            )
            self.batchers[address] = batcher
        return batcher

    def _until_tick(self, address: str, interval: float, earliest: float) -> float:
        """Seconds from now to this device's first poll tick after loop time
        `earliest`. Ticks are fixed multiples of the interval (offset by the
        device's phase), so points that drift apart - one backed off after a
        failure, one restarted by an update - land back on the same tick and
        in the same RPM."""
        loop_now = asyncio.get_running_loop().time()
        base = self._phase.setdefault(address, random.random()) * interval
        tick = base + (math.floor((earliest - base) / interval) + 1) * interval
        return max(0.0, tick - loop_now)

    def _start_poll_task(self, record: dict) -> None:
        task = asyncio.get_event_loop().create_task(self._poll_loop(record))
        task.add_done_callback(self._make_crash_logger(record["id"]))
        self.tasks[record["id"]] = task

    def _make_crash_logger(self, connection_id: str):
        """An unhandled exception in an asyncio.Task otherwise dies
        silently - exactly the shape of the Haxall task that erred 34
        times with zero visible signal and is the reason this project
        mandates tracing upfront. Built in from the first commit of this
        module, not added after finding a dead connection in practice."""

        def on_done(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is None:
                return
            print(f"connection {connection_id} poll task crashed: {exc!r}")
            with tracer.start_as_current_span("connection_manager.poll_task_crashed") as span:
                span.set_attribute("connection_id", connection_id)
                span.set_attribute("error", str(exc))

        return on_done

    async def _poll_loop(self, record: dict) -> None:
        """One asyncio task per point, not one shared loop over all of
        them - the only way a point can have its own poll_interval/
        hisCollect policy independent of its siblings on the same
        connection. Cancelling this task (delete_connection) must
        explicitly cancel the per-point sub-tasks too: asyncio.gather()
        does NOT auto-cancel its children just because the task awaiting
        it was cancelled - confirmed, not assumed."""
        states = {point["label"]: _PointState() for point in record["points"]}
        point_tasks = [
            asyncio.get_event_loop().create_task(self._poll_point(record, point, states[point["label"]]))
            for point in record["points"]
        ]
        if record.get("forward_alarms"):
            point_tasks.append(asyncio.get_event_loop().create_task(self._keep_alarms(record)))
        if record.get("priority_interval", DEFAULT_PRIORITY_INTERVAL) and any(
            _object_type(p["object_identifier"]) in COMMANDABLE_TYPES for p in record["points"]
        ):
            point_tasks.append(asyncio.get_event_loop().create_task(self._keep_priorities(record, states)))
        try:
            await asyncio.gather(*point_tasks)
        except asyncio.CancelledError:
            for point_task in point_tasks:
                point_task.cancel()
            raise

    async def _poll_point(self, record: dict, point: dict, state: "_PointState | None" = None) -> None:
        """Deliberately a small, separate near-duplicate of bridge.py's
        poll_forever rather than a forced shared abstraction: it iterates
        a dynamic list-of-dicts from a stored record instead of a static
        CLI arg list. Sharing normalize_value and publish_reading is the
        right amount of sharing; sharing this loop body isn't.

        Reads go through the device's ReadBatcher (one RPM for every point
        on the device that ticks together) unless the connection sets
        read_multiple: false. With subscribe_cov, a sibling task keeps a
        COV subscription alive; while it is, this loop only polls every
        cov_poll_interval, as a safety net for a lost notification or a
        lapsed subscription.

        A point with `trend_log` backfills (Baler#129) on its first good
        read - from the last reading taken before a restart, per the marks
        file - and on the first good read after failures, from the last good
        read before them."""
        address = record["device_address"]
        interval = point.get("poll_interval", record["poll_interval"])
        batched = record.get("read_multiple", True)
        with_quality = record.get("read_quality", True)
        cov_poll_interval = record.get("cov_poll_interval", DEFAULT_COV_POLL_INTERVAL)
        state = state if state is not None else _PointState()
        trended = "trend_log" in point
        last_ok: float | None = self._marks.get(f"{record.get('id')}/{point['label']}") if trended else None
        backfills: set[asyncio.Task] = set()
        cov_task = None
        if point.get("subscribe_cov", record.get("subscribe_cov", False)):
            cov_task = asyncio.get_running_loop().create_task(self._keep_cov(record, point, state, interval))

        async def wait_next(every: float, failures: int) -> None:
            delay = failure_backoff(every, failures)
            if batched:
                loop_now = asyncio.get_running_loop().time()
                delay = self._until_tick(address, every, loop_now + delay - every)
            if not state.wake.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(state.wake.wait(), delay)
            state.wake.clear()

        try:
            # Spread first reads. Batched: onto the device's tick, so the
            # whole device is one burst of a few RPMs rather than one read
            # per point. Unbatched: uniformly over an interval - on a warm
            # restore every point's first read otherwise fires in the same
            # tick, which at ~500 points livelocked the shared bacpypes3
            # Application (confirmed live).
            if batched:
                await asyncio.sleep(self._until_tick(address, interval, asyncio.get_running_loop().time()))
            else:
                await asyncio.sleep(random.uniform(0, interval))

            failures = 0
            first_read = True
            while True:
                try:
                    if batched:
                        raw = await self._batcher(address).read(point["object_identifier"], status=with_quality)
                    else:
                        async with self._slots():
                            self.stats["read_property"] += 1
                            raw = await read_present_value(self.app, address, point["object_identifier"])
                    quality = None
                    if isinstance(raw, Sample):
                        raw, quality = raw.value, (raw.quality if with_quality else None)
                    if raw is None or isinstance(raw, (ErrorRejectAbortNack, ErrorType)):
                        raise ReadFailed(f"device answered {raw!r}")
                    value = normalize_value(raw)
                except Exception as exc:  # noqa: BLE001 - every failure means "count it and retry"
                    # A failed read publishes nothing: a null reading made a dead
                    # device look like it was reporting downstream (Baler#83/#85).
                    failures += 1
                    self.status.failure(record.get("id"), point["label"], exc)
                    if failures in (1, 5) or failures % 50 == 0:  # don't flood the log with a dead device
                        print(f"read failed for {point['object_identifier']} ({failures}x): {exc!r}")
                    if failures % ROUTE_RESET_FAILURES == 0 and bacnet_network.forget_route(self.app, address):
                        print(f"forgot the route to {address} after {failures} failures; looking for its router again")
                    await wait_next(interval, failures)
                    continue
                if failures:
                    print(f"read recovered for {point['object_identifier']} after {failures} failures")
                    # a device that went away has probably dropped its subscriptions too
                    state.resubscribe.set()
                read_at = time.time()
                if trended and (first_read or failures) and last_ok is not None:
                    # what the controller logged while we weren't reading it
                    task = asyncio.get_running_loop().create_task(self._backfill(record, point, last_ok, read_at))
                    backfills.add(task)
                    task.add_done_callback(backfills.discard)
                first_read = False
                last_ok = read_at
                failures = 0

                self._offer(record, point, state, value, quality)
                await wait_next(cov_poll_interval if state.cov_active else interval, 0)
        finally:
            if cov_task is not None:
                cov_task.cancel()
            for task in backfills:
                task.cancel()

    def _offer(self, record: dict, point: dict, state: _PointState, value, quality: dict | None = None) -> None:
        """A fresh value, from a poll or a COV notification: publish it if the
        point's hisCollect policy says so, or if its quality changed (a
        sensor failing at the same value is news). Either way it counts as a
        good read in the connection's poll status (Baler#110)."""
        now = time.monotonic()
        # a change in who holds the point (Windrow#25), or in its quality, is
        # news even at the same value
        publish = state.priority != state.last_priority or quality != state.last_quality or should_publish(
            value,
            state.last_value,
            state.last_published_at,
            now,
            his_collect_cov=point.get("his_collect_cov"),
            his_collect_cov_rate_limit=point.get("his_collect_cov_rate_limit"),
            his_collect_interval=point.get("his_collect_interval"),
        )
        if publish:
            # `quality` only when there is some, `priority` only for a point
            # whose array has been read: every other call (and envelope) is
            # exactly what it was before Windrow#20/#25
            extra: dict = {"quality": quality} if quality else {}
            if state.priority is not None:
                extra["priority"] = state.priority
            mqtt_sink.publish_live(self.mqtt_client, record["topic_prefix"], point["label"], value, **extra)
            state.last_value = value
            state.last_quality = quality
            state.last_priority = state.priority
            state.last_published_at = now
        self.status.success(record.get("id"), point["label"], published=publish)
        self._mark(record, point, time.time())

    # --- priority arrays (Windrow#25) ---

    async def _keep_priorities(self, record: dict, states: dict) -> None:
        """Read every commandable point's priority-array each
        priority_interval and, when it changed, wake the point's poll so
        its next reading carries the new `priority`. One RPM per
        PRIORITY_BATCH points (halved on an answer too big; single reads
        for a device without RPM). A point whose device says it has no
        priority-array isn't asked again. Read-only, and never ends the
        poll loop it runs in."""
        address = record["device_address"]
        interval = record.get("priority_interval", DEFAULT_PRIORITY_INTERVAL)
        asking = {p["object_identifier"]: p["label"] for p in record["points"]
                  if _object_type(p["object_identifier"]) in COMMANDABLE_TYPES}
        wake = self._priority_wake.setdefault(record["id"], asyncio.Event())
        batch = PRIORITY_BATCH
        rpm = record.get("read_multiple", True)
        failures = 0
        await asyncio.sleep(random.uniform(0, min(interval, 5.0)))
        while asking:
            try:
                arrays: dict = {}
                objids = list(asking)
                for start in range(0, len(objids), batch):
                    chunk = objids[start:start + batch]
                    try:
                        if not rpm or len(chunk) == 1:
                            raise RPMUnsupported("single reads")
                        async with self._slots():
                            self.stats["read_priority_array"] += 1
                            arrays.update(await read_present_values(self.app, address, chunk, prop="priority-array"))
                    except RPMUnsupported as exc:
                        if exc.too_big and batch > 1:
                            batch = max(1, batch // 2)
                        elif str(exc) != "single reads":
                            rpm = False
                        for objid in chunk:
                            async with self._slots():
                                self.stats["read_priority_array"] += 1
                                arrays[objid] = await read_present_value(self.app, address, objid, prop="priority-array")
                for objid, raw in arrays.items():
                    if isinstance(raw, (ErrorRejectAbortNack, ErrorType)):
                        # unknown-property: this object has no priority array
                        print(f"{address} {objid} has no priority-array ({raw}); not asking again")
                        asking.pop(objid, None)
                        continue
                    levels = priority_levels(raw)
                    state = states.get(asking.get(objid))
                    if levels is not None and state is not None and levels != state.priority:
                        state.priority = levels
                        state.wake.set()  # publish it with a fresh value now
                if failures:
                    print(f"priority-array reads on {address} recovered after {failures} failures")
                failures = 0
                delay = interval
            except Exception as exc:  # noqa: BLE001 - retried with back-off; polling carries on
                failures += 1
                if failures in (1, 5) or failures % 50 == 0:
                    print(f"priority-array read on {address} failed ({failures}x): {exc!r}")
                delay = failure_backoff(interval, failures)
            wake.clear()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wake.wait(), delay)

    # --- alarms (Baler#164) ---

    def _background(self, coro) -> None:
        """Fire and forget, but keep a reference and log a failure."""
        task = asyncio.get_running_loop().create_task(coro)
        self._side_tasks.add(task)

        def done(t: asyncio.Task) -> None:
            self._side_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                print(f"background task failed: {t.exception()!r}")

        task.add_done_callback(done)

    async def _keep_alarms(self, record: dict) -> None:
        """Register in every notification class on the device, publish what's
        already in alarm, and do both again every ALARM_REFRESH_INTERVAL (and
        with back-off after a failure). Never ends the poll loop it runs in."""
        address = record["device_address"]
        failures = 0
        while True:
            try:
                async with self._slots():
                    classes = await bacnet_alarms.notification_classes(self.app, address, record["device_instance"])
                for nc in classes:
                    async with self._slots():
                        if await bacnet_alarms.ensure_recipient(self.app, address, nc):
                            print(f"registered for alarms from {address} {nc}")
                async with self._slots():
                    active = await bacnet_alarms.get_event_information(self.app, address)
                for event in active:
                    self._publish_alarm(record, event["object"], {**event, "ts": time.time(), "source": "summary"})
                if failures:
                    print(f"alarm setup for {address} recovered after {failures} failures")
                failures = 0
                if not classes:
                    print(f"{address} has no notification-class objects; no alarms to forward")
                delay = ALARM_REFRESH_INTERVAL
            except Exception as exc:  # noqa: BLE001 - retried; polling carries on regardless
                failures += 1
                if failures in (1, 5) or failures % 50 == 0:
                    print(f"alarm setup for {address} failed ({failures}x): {exc!r}")
                delay = failure_backoff(30.0, failures)
            await asyncio.sleep(delay)

    async def _unregister_alarms(self, record: dict) -> None:
        address = record["device_address"]
        try:
            for nc in await bacnet_alarms.notification_classes(self.app, address, record["device_instance"]):
                await bacnet_alarms.remove_recipient(self.app, address, nc)
        except Exception as exc:  # noqa: BLE001 - best effort; the entry is harmless if left behind
            print(f"couldn't remove the alarm recipient entries on {address}: {exc!r}")

    def on_event_notification(self, apdu) -> None:
        """An EventNotification from a device: publish it on every
        forward_alarms connection to that device."""
        self.stats["event_notifications"] += 1
        source = bacnet_network.address_key(str(apdu.pduSource))
        event = bacnet_alarms.notification_to_dict(apdu)
        matched = False
        for record in self.connections.values():
            if record.get("forward_alarms") and bacnet_network.address_key(record["device_address"]) == source:
                self._publish_alarm(record, event["object"], event)
                matched = True
        if not matched:
            print(f"event notification from {apdu.pduSource} for {event['object']} matches no forward_alarms connection")

    def _publish_alarm(self, record: dict, objid: str, event: dict) -> None:
        """Retained, so a subscriber sees the current state: on the point's
        `{label}/alarm` when the object is one of the connection's points,
        else on `alarms/{type}-{instance}` (a controller alarms on objects
        nobody polls, e.g. a fault on a device-level object)."""
        label = None
        for point in record["points"]:
            if _same_object(point["object_identifier"], objid):
                label = point["label"]
                break
        topic = f"{label}/alarm" if label else "alarms/" + objid.replace(",", "-")
        mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], topic, event, ts=event.get("ts"))

    async def active_alarms(self, connection_id: str) -> list[dict] | None:
        """The device's active alarms now (GetEventInformation), for the API."""
        record = self.connections.get(connection_id)
        if record is None:
            return None
        async with self._slots():
            return await bacnet_alarms.get_event_information(self.app, record["device_address"])

    def _set_cov_live(self, state: _PointState, live: bool) -> None:
        if state.cov_active == live:
            return
        state.cov_active = live
        if live:
            self._cov_live.add(id(state))
        else:
            self._cov_live.discard(id(state))
            state.wake.set()  # back to the normal rate now, not after the slow poll's long sleep

    async def _keep_cov(self, record: dict, point: dict, state: _PointState, interval: float) -> None:
        """Subscribe, renew at half the lifetime, and on any failure fall back
        to polling at the point's normal interval while retrying with
        back-off. A device that refuses COV for this object is polled from
        then on and not asked again (until the connection restarts)."""
        objid = point["object_identifier"]
        lifetime = record.get("cov_lifetime", DEFAULT_COV_LIFETIME)

        def on_value(raw, quality=None) -> None:
            self.stats["cov_notifications"] += 1
            self._offer(record, point, state, normalize_value(raw), quality if record.get("read_quality", True) else None)

        subscription = bacnet_cov.Subscription(self.app, record["device_address"], objid, on_value)
        failures = 0
        try:
            await asyncio.sleep(random.uniform(0, min(interval, 5.0)))  # don't subscribe a whole site in one tick
            while True:
                try:
                    async with self._slots():
                        self.stats["subscribe_cov"] += 1
                        await subscription.subscribe(lifetime)
                except bacnet_cov.CovUnsupported as exc:
                    print(f"COV not available for {objid} ({exc}); polling it every {interval:g}s")
                    self._set_cov_live(state, False)
                    return
                except Exception as exc:  # noqa: BLE001 - poll meanwhile, retry with back-off
                    failures += 1
                    if failures in (1, 5) or failures % 50 == 0:
                        print(f"COV subscribe failed for {objid} ({failures}x): {exc!r}; polling every {interval:g}s")
                    self._set_cov_live(state, False)
                    delay = failure_backoff(min(lifetime / 2, 30.0), failures)
                else:
                    if failures or not state.cov_active:
                        print(f"COV subscribed for {objid} (lifetime {lifetime}s)")
                    failures = 0
                    self._set_cov_live(state, True)
                    delay = max(1.0, lifetime / 2)
                state.resubscribe.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(state.resubscribe.wait(), delay)
        finally:
            self._cov_live.discard(id(state))
            subscription.close()


def _same_object(a: str, b: str) -> bool:
    """`analog-value,1` and `analogValue,1` name the same object."""
    from bacpypes3.primitivedata import ObjectIdentifier

    try:
        return ObjectIdentifier(a) == ObjectIdentifier(b)
    except Exception:  # noqa: BLE001 - unparseable: compare the text
        return a.replace(" ", "") == b.replace(" ", "")


def _load_marks(path: str) -> dict[str, float]:
    try:
        with open(path) as f:
            marks = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        print(f"ignoring unreadable trend marks file {path}: {exc!r}")
        return {}
    return {k: float(v) for k, v in marks.items() if isinstance(v, (int, float))}


def _save_marks(path: str, marks: dict[str, float]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(marks, f)
    os.replace(tmp, path)


def _objid(text: str):
    """`analogValue,1` and `analog-value,1` alike, as bacpypes3 prints them."""
    from bacpypes3.primitivedata import ObjectIdentifier

    try:
        return ObjectIdentifier(text)
    except Exception:  # noqa: BLE001 - unparseable: keep the text, it just won't match
        return text


def _to_bacnet(value, obj_type: str):
    """Haystack-side value -> what bacpypes3 encodes for present-value.
    None = relinquish (BACnet Null). Binary objects take active/inactive."""
    from bacpypes3.primitivedata import Null

    if value is None:
        return Null(())
    if obj_type.startswith("binary"):
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("active", "on", "true", "1"):
                return "active"
            if v in ("inactive", "off", "false", "0"):
                return "inactive"
            raise PointWriteError(f"can't write {value!r} to a binary point")
        return "active" if value else "inactive"
    if obj_type.startswith("multi-state"):
        if isinstance(value, bool) or not float(value).is_integer() or value < 1:
            raise PointWriteError(f"multi-state points take a state number >= 1, got {value!r}")
        return int(value)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PointWriteError(f"can't write {value!r} to an analog point") from exc
