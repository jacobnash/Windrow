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
import os
import random
import time

from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.app import Application

from windrow import (
    bacnet_network,
    bacnet_tagger,
    connection_store,
    discovery,
    mqtt_sink,
    tracing,
)
from windrow.bacnet_read import read_present_value
from windrow.bacnet_values import normalize_value

tracer = tracing.get_tracer(__name__)

_TAGGER_RULES_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "rules", "bacnet_to_haystack_tags.yaml")
_TAGGER_RULES = bacnet_tagger.load_rules(_TAGGER_RULES_PATH)


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
}
UPDATABLE_FIELDS = {"points", "poll_interval", "topic_prefix"}


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
        cov = point.get("his_collect_cov")
        if cov is not None and not isinstance(cov, (bool, int, float)):
            raise ValueError(f"his_collect_cov must be true/false or a number, got {cov!r}")
        for key in ("his_collect_cov_rate_limit", "his_collect_interval"):
            if point.get(key) is not None:
                connection_store.check_number(key, point[key])
        cleaned.append({k: v for k, v in point.items() if k in POINT_FIELDS})
    return cleaned


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


def _object_type(object_identifier: str) -> str:
    return str(object_identifier).split(",", 1)[0].strip()


# Reads in flight at once across every connection (Baler#47). bacpypes3
# runs on the same event loop as the HTTP API's calls; hundreds of
# outstanding reads - most of them to devices that no longer answer, each
# waiting out its APDU timeout and retrying at once - starved the loop
# until every API call timed out.
MAX_CONCURRENT_READS = int(os.environ.get("WINDROW_MAX_CONCURRENT_READS", "16"))
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
        self._read_slots: asyncio.Semaphore | None = None

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

    async def create_connection(
        self,
        device_address: str,
        device_instance: int,
        topic_prefix: str,
        points: list[dict],
        poll_interval: float = 5.0,
        credential: dict | None = None,
    ) -> dict:
        with tracer.start_as_current_span("connection_manager.create_connection") as span:
            bacnet_network.parse_device_address(device_address)  # ValueError (a 400) for a broadcast or a typo
            record = {
                "id": connection_store.new_connection_id(),
                "device_address": device_address,
                "device_instance": device_instance,
                "topic_prefix": topic_prefix,
                "poll_interval": poll_interval,
                "points": points,
                "credential": credential,
            }
            self.connections[record["id"]] = record
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
            if priority == 17:
                await self.app.write_property(
                    record["device_address"], point["object_identifier"], "relinquish-default", bacnet_value
                )
            else:
                await self.app.write_property(
                    record["device_address"], point["object_identifier"], "present-value", bacnet_value, priority=priority
                )
            present = normalize_value(
                await self.app.read_property(record["device_address"], point["object_identifier"], "present-value")
            )
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
            del self.connections[connection_id]
            self._save()
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

            await stop_task(self.tasks.pop(connection_id, None))
            self.connections[connection_id] = record
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
        point_tasks = [asyncio.get_event_loop().create_task(self._poll_point(record, point)) for point in record["points"]]
        try:
            await asyncio.gather(*point_tasks)
        except asyncio.CancelledError:
            for point_task in point_tasks:
                point_task.cancel()
            raise

    async def _poll_point(self, record: dict, point: dict) -> None:
        """Deliberately a small, separate near-duplicate of bridge.py's
        poll_forever rather than a forced shared abstraction: it iterates
        a dynamic list-of-dicts from a stored record instead of a static
        CLI arg list. Sharing normalize_value and publish_reading is the
        right amount of sharing; sharing this loop body isn't."""
        interval = point.get("poll_interval", record["poll_interval"])
        last_value = None
        last_published_at: float | None = None

        # Jittered start: on a warm restore (connection_manager.start()),
        # every point's task fires its first read_property in the same
        # event-loop tick. Confirmed live: at ~500 points this burst
        # livelocks the single shared bacpypes3 Application (pegs a core,
        # zero reads actually complete) even though the same reads work
        # fine sequentially. Spreading first reads across one interval
        # keeps steady-state polling identical.
        await asyncio.sleep(random.uniform(0, interval))

        failures = 0
        while True:
            try:
                async with self._slots():
                    raw = await read_present_value(self.app, record["device_address"], point["object_identifier"])
                if raw is None or isinstance(raw, ErrorRejectAbortNack):
                    raise ReadFailed(f"device answered {raw!r}")
                value = normalize_value(raw)
            except Exception as exc:  # noqa: BLE001 - every failure means "count it and retry"
                # A failed read publishes nothing: a null reading made a dead
                # device look like it was reporting downstream (Baler#83/#85).
                failures += 1
                if failures in (1, 5) or failures % 50 == 0:  # don't flood the log with a dead device
                    print(f"read failed for {point['object_identifier']} ({failures}x): {exc!r}")
                if failures % ROUTE_RESET_FAILURES == 0 and bacnet_network.forget_route(self.app, record["device_address"]):
                    print(f"forgot the route to {record['device_address']} after {failures} failures; looking for its router again")
                await asyncio.sleep(failure_backoff(interval, failures))
                continue
            if failures:
                print(f"read recovered for {point['object_identifier']} after {failures} failures")
            failures = 0

            now = time.monotonic()
            if should_publish(
                value,
                last_value,
                last_published_at,
                now,
                his_collect_cov=point.get("his_collect_cov"),
                his_collect_cov_rate_limit=point.get("his_collect_cov_rate_limit"),
                his_collect_interval=point.get("his_collect_interval"),
            ):
                mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], point["label"], value)
                last_value = value
                last_published_at = now

            await asyncio.sleep(failure_backoff(interval, failures))


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
