"""
Modbus counterpart to connection_manager.py - deliberately smaller, not full
parity. Modbus has no shared Application (one AsyncModbusTcpClient per
connection, same as modbus_bridge.py already does) and no learn() (Modbus
has no self-describing object-list the way BACnet's object-list property
does - register addresses are opaque and vendor-documented, so a human
supplies {label: address} points at connection-creation time, same shape
modbus_bridge.py's --points flag already produces via parse_points()). A
point can also be an object naming its function code, data type, word/byte
order, scale/offset, unit and whether it's writable - see modbus_points.py
(Baler#128).

One poll task per *connection*, not per point: modbus_bridge.poll_forever's
loop-over-all-points-per-interval body, adapted to a configurable interval
and made cancellable, rather than reproducing BACnet's per-point-task/COV
machinery - nothing here asked for per-point poll intervals on Modbus, and
building that would be over-parity for no requirement driving it.
"""

import asyncio
import contextlib
import json
import time

import pymodbus.client as ModbusClient

from windrow import bacnet_tagger, connection_store, modbus_points, mqtt_sink, tracing
from windrow.poll_status import PollStatus

tracer = tracing.get_tracer(__name__)

# The same label -> Haystack markers rules the BACnet connections use: a
# Modbus point has no object type or description, but its label carries the
# same words ("zone_temp", "fan_status").
_TAGGER_RULES = bacnet_tagger.load_default_rules()

# Longest wait between reconnect attempts to an unreachable device.
RECONNECT_MAX = 60.0

UPDATABLE_FIELDS = {"points", "poll_interval", "topic_prefix", "host", "port", "device_id", "units"}

# A test read (Baler#110) answers within this, reachable device or not.
TEST_READ_TIMEOUT = 5.0


_READ_METHODS = {
    "holding": "read_holding_registers",  # FC3
    "input": "read_input_registers",  # FC4
    "coil": "read_coils",  # FC1
    "discrete": "read_discrete_inputs",  # FC2
}

# Haystack/BACnet priority levels; 17 is the default (relinquish-default).
LEVELS = range(1, 18)


class PointWriteError(ValueError):
    """A write refused before or by the device - a 400 / ok:false, like
    connection_manager.PointWriteError for BACnet."""


def validate_points(points) -> dict[str, int | dict]:
    """{label: register address or point object}, non-empty. A bare
    address is the original holding-register form and is stored as-is;
    modbus_points.validate_point checks the object form."""
    if not isinstance(points, dict) or not points:
        raise ValueError("points must be a non-empty {label: register_address or point object} object")
    out: dict[str, int | dict] = {}
    for label, spec in points.items():
        connection_store.check_text("point label", label)
        if "+" in label or "#" in label:
            raise ValueError(f"point label {label!r} can't contain MQTT wildcards (+ or #)")
        out[label] = modbus_points.validate_point(label, spec)
    return out


def validate_units(units, points) -> dict[str, str]:
    """Optional {label: Haystack unit} for some of the points (Baler#82).
    A point object can carry its own `unit` instead; naming one point's
    unit in both places is refused rather than picking a winner."""
    if units is None:
        return {}
    if not isinstance(units, dict):
        raise ValueError("units must be a {label: unit} object")  # noqa: TRY004 - api.py answers ValueError with 400
    for label, unit in units.items():
        if label not in points:
            raise ValueError(f"units names {label!r}, which is not one of the points")
        if not isinstance(unit, str) or not unit.strip():
            raise ValueError(f"unit for {label!r} must be a non-empty string")
        if isinstance(points[label], dict) and "unit" in points[label]:
            raise ValueError(f"{label!r} has a unit on the point itself; don't also name it in units")
    return {label: unit.strip() for label, unit in units.items()}


def _effective(levels: dict[str, object]):
    """(level, value) of the highest-priority (lowest-numbered) level set."""
    if not levels:
        return None, None
    level = min(levels, key=int)
    return int(level), levels[level]


class ModbusConnectionManager:
    def __init__(self, mqtt_client, state_file: str, site_id: str | None = None, instance_id: str | None = None):
        self.mqtt_client = mqtt_client
        self.state_file = state_file
        self.connections: dict[str, dict] = {}
        self.clients: dict[str, ModbusClient.AsyncModbusTcpClient] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        # One request at a time per device: a write mustn't interleave
        # with the poll loop's reads on the same client.
        self.locks: dict[str, asyncio.Lock] = {}
        # Advertised on the equipment record as windrowSite/windrowInstance
        # when the command channel is on, so Baler can route point writes
        # here - same as ConnectionManager does for BACnet.
        self.site_id = site_id
        self.instance_id = instance_id
        # In-memory poll health per connection, never persisted (Baler#110).
        self.status = PollStatus()

    async def start(self) -> None:
        with tracer.start_as_current_span("modbus_connection_manager.start") as span:
            records = connection_store.load(self.state_file)
            for record in records:
                self.connections[record["id"]] = record
                await self._start_poll_task(record)
                # Retained, but republished so connections made before
                # this release get their equipment record too (Baler#80).
                self._publish_tags(record)
            span.set_attribute("restored_count", len(records))

    async def create_connection(
        self,
        host: str,
        topic_prefix: str,
        points: dict[str, int | dict],
        port: int = 502,
        device_id: int = 1,
        poll_interval: float = 5.0,
        credential: dict | None = None,
        units: dict[str, str] | None = None,
    ) -> dict:
        with tracer.start_as_current_span("modbus_connection_manager.create_connection") as span:
            points = validate_points(points)
            units = validate_units(units, points)
            record = {
                "id": connection_store.new_connection_id(),
                "host": host,
                "port": port,
                "device_id": device_id,
                "topic_prefix": topic_prefix,
                "poll_interval": poll_interval,
                "points": points,
                "credential": credential,
            }
            if units:
                record["units"] = units
            self.connections[record["id"]] = record
            self._save()
            self._publish_tags(record)
            await self._start_poll_task(record)
            span.set_attribute("connection_id", record["id"])
            span.set_attribute("point_count", len(points))
            return record

    def _publish_tags(self, record: dict) -> None:
        """What connection_manager's _tag_points + _publish_equip_membership
        do for BACnet (Baler#80): {prefix}/{label}/tags per point and one
        {prefix}/equip/tags naming them, both retained. Without these Baler
        made the points but no equipment, so they never appeared in its
        equipment tree. Equipment identity is the topic_prefix, as for
        BACnet. A point's markers come from its label; its `unit` from the
        point object or the connection's units map (a register is just a
        number); `writable` from the point object (Baler#128) - Baler only
        accepts a pointWrite to a point tagged writable, and routes it by
        the equipment's windrowSite/windrowInstance."""
        with tracer.start_as_current_span("modbus_connection_manager.publish_tags") as span:
            prefix = record["topic_prefix"]
            units = record.get("units") or {}
            for label, spec in record["points"].items():
                point = modbus_points.resolve(spec)
                tags: dict = dict(bacnet_tagger.tag_point({"label": label}, _TAGGER_RULES))
                if point["writable"]:
                    tags["writable"] = True
                    tags.pop("sensor", None)  # Haystack: a commanded point isn't a sensor
                unit = point.get("unit") or units.get(label)
                if unit:
                    tags["unit"] = unit
                mqtt_sink.publish_reading(self.mqtt_client, prefix, f"{label}/tags", json.dumps(tags))
            equip_tags = {"windrowSite": self.site_id, "windrowInstance": self.instance_id} if self.site_id else {}
            equip = {"tags": equip_tags, "points": list(record["points"])}
            mqtt_sink.publish_reading(self.mqtt_client, prefix, "equip/tags", json.dumps(equip))
            span.set_attribute("point_count", len(record["points"]))

    async def delete_connection(self, connection_id: str) -> bool:
        with tracer.start_as_current_span("modbus_connection_manager.delete_connection") as span:
            span.set_attribute("connection_id", connection_id)
            if connection_id not in self.connections:
                span.set_attribute("found", False)
                return False

            task = self.tasks.pop(connection_id, None)
            if task is not None:
                task.cancel()
            client = self.clients.pop(connection_id, None)
            if client is not None:
                client.close()
            del self.connections[connection_id]
            self.status.reset(connection_id)
            self._save()
            span.set_attribute("found", True)
            return True

    async def update_connection(self, connection_id: str, patch: dict) -> dict | None:
        """Edit any of points/poll_interval/topic_prefix/host/port/device_id:
        validate, persist, then stop the poll task and close its client
        before starting a fresh one - the old client is bound to the old
        host/port, and a client left open keeps pymodbus's own tasks alive
        (see close()). Points that no longer publish where they did (all of
        them when the prefix moves) get their retained value and tags
        topics cleared, and the tags (Baler#80) republished for the new shape."""
        with tracer.start_as_current_span("modbus_connection_manager.update_connection") as span:
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
            if "host" in patch:
                record["host"] = connection_store.check_text("host", patch["host"]).strip()
            if "port" in patch:
                record["port"] = connection_store.check_int("port", patch["port"], 1, 65535)
            if "device_id" in patch:
                record["device_id"] = connection_store.check_int("device_id", patch["device_id"], 0, 255)
            if "units" in patch or "points" in patch:
                # A patch's units replace the map; otherwise keep the units
                # of the points that survived a points edit.
                units = patch["units"] if "units" in patch else {
                    label: unit for label, unit in (old.get("units") or {}).items()
                    if label in record["points"]
                    and not (isinstance(record["points"][label], dict) and "unit" in record["points"][label])
                }
                units = validate_units(units, record["points"])
                if units:
                    record["units"] = units
                else:
                    record.pop("units", None)
            if "points" in patch or "topic_prefix" in patch:
                self._prune_write_levels(record)

            task = self.tasks.pop(connection_id, None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            client = self.clients.pop(connection_id, None)
            if client is not None:
                client.close()
            self.status.reset(connection_id)
            self.connections[connection_id] = record
            self._save()

            if record["topic_prefix"] != old["topic_prefix"]:
                removed = list(old["points"])
            else:
                removed = [label for label in old["points"] if label not in record["points"]]
            for label in removed:
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], label)
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], f"{label}/tags")
            if record["topic_prefix"] != old["topic_prefix"]:
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], "equip/tags")
            self._publish_tags(record)

            await self._start_poll_task(record)
            span.set_attribute("found", True)
            span.set_attribute("cleared_count", len(removed))
            return record

    @staticmethod
    def _prune_write_levels(record: dict) -> None:
        """Commanded levels (see write_point) survive an edit only for
        points that still exist, are still writable and still publish on
        the same topic - otherwise nothing could ever relinquish them."""
        levels = record.get("write_levels") or {}
        kept = {
            label: lv for label, lv in levels.items()
            if label in record["points"] and modbus_points.resolve(record["points"][label])["writable"]
        }
        if kept:
            record["write_levels"] = kept
        else:
            record.pop("write_levels", None)

    def _find_point(self, topic: str) -> tuple[dict, str]:
        for record in self.connections.values():
            prefix = record["topic_prefix"].rstrip("/") + "/"
            if topic.startswith(prefix) and topic[len(prefix):] in record["points"]:
                return record, topic[len(prefix):]
        raise PointWriteError(f"no Modbus point publishes {topic!r}")

    def owns(self, topic) -> bool:
        """Whether `topic` is one of this manager's points - api.py's
        point_write sends it here instead of to the BACnet manager."""
        try:
            self._find_point(topic)
        except PointWriteError:
            return False
        return True

    def _lock(self, connection_id: str) -> asyncio.Lock:
        return self.locks.setdefault(connection_id, asyncio.Lock())

    async def write_point(self, topic: str, value, priority: int, expires_at: float | None = None) -> dict:
        """Command a writable holding register or coil (Baler#128), on the
        same command shape and expiry rule as BACnet's write_point.

        Modbus has no priority array, so this keeps one per point (persisted
        on the connection as `write_levels`) and writes the highest-priority
        level's value to the device - the same thing a BACnet controller
        does with its own array. A null value relinquishes that level and
        the next one down is written; with every level relinquished nothing
        is written and the register keeps its last value (there's no
        relinquish-default on the device; level 17 plays that role when
        set). The value is read back and published straight away."""
        with tracer.start_as_current_span("modbus_connection_manager.write_point") as span:
            span.set_attribute("topic", topic)
            span.set_attribute("priority", str(priority))
            if expires_at is not None and time.time() > expires_at:
                raise PointWriteError("command expired before it reached the site; not applied")
            if not isinstance(priority, int) or isinstance(priority, bool) or priority not in LEVELS:
                raise PointWriteError(f"priority must be 1-17, got {priority!r}")
            if priority == 17 and value is None:
                raise PointWriteError("level 17 (default) can be set but not relinquished")
            record, label = self._find_point(topic)
            point = modbus_points.resolve(record["points"][label])
            if not point["writable"]:
                raise PointWriteError(f"{topic} isn't writable (its point needs \"writable\": true)")
            if value is not None:
                try:
                    modbus_points.encode(point, value)
                except ValueError as exc:
                    raise PointWriteError(f"{topic}: {exc}") from exc

            levels = dict((record.get("write_levels") or {}).get(label, {}))
            if value is None:
                levels.pop(str(priority), None)
            else:
                levels[str(priority)] = value
            level, effective = _effective(levels)

            async with self._lock(record["id"]):
                client = self.clients.get(record["id"])
                if client is None or not client.connected:
                    raise PointWriteError(f"Modbus device {record['host']}:{record['port']} isn't connected right now")
                try:
                    if effective is not None:
                        await self._write(client, record, point, modbus_points.encode(point, effective))
                    present = await self._read(client, record, point)
                except PointWriteError:
                    raise
                except Exception as exc:  # a lost session raises many types
                    raise PointWriteError(f"Modbus write to {topic} failed: {exc!r}") from exc

            # Only a write the device accepted changes the recorded array -
            # on the connection as it is now, in case an edit replaced it.
            record = self.connections.get(record["id"], record)
            write_levels = dict(record.get("write_levels") or {})
            if levels:
                write_levels[label] = levels
            else:
                write_levels.pop(label, None)
            if write_levels:
                record["write_levels"] = write_levels
            else:
                record.pop("write_levels", None)
            self._save()
            if present is not None:
                mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], label, present)
            span.set_attribute("effective_level", str(level))
            return {"topic": topic, "priority": priority, "value": value, "present_value": present,
                    "effective_level": level}

    async def _write(self, client, record: dict, point: dict, encoded) -> None:
        device_id = record["device_id"]
        if point["function"] == "coil":
            rr = await client.write_coil(point["register"], encoded, device_id=device_id)
        elif len(encoded) == 1:
            rr = await client.write_register(point["register"], encoded[0], device_id=device_id)
        else:
            rr = await client.write_registers(point["register"], encoded, device_id=device_id)
        if rr.isError():
            raise PointWriteError(f"device refused the write: {rr}")

    async def _read(self, client, record: dict, point: dict):
        """One point's decoded value, or None if the device answered with
        an exception code. Raises if the session itself failed."""
        read = getattr(client, _READ_METHODS[point["function"]])
        rr = await read(point["register"], count=point["count"], device_id=record["device_id"])
        if rr.isError():
            print(f"modbus read error for {point['function']}@{point['register']}: {rr}")
            return None
        words = rr.bits if point["function"] in modbus_points.BIT_FUNCTIONS else rr.registers
        if len(words) < point["count"]:
            print(f"modbus read short for {point['function']}@{point['register']}: {rr}")
            return None
        return modbus_points.decode(point, words)

    async def list_connections(self) -> list[dict]:
        with tracer.start_as_current_span("modbus_connection_manager.list_connections") as span:
            connections = list(self.connections.values())
            span.set_attribute("count", len(connections))
            return connections

    async def set_credential(self, connection_id: str, credential: dict) -> dict | None:
        with tracer.start_as_current_span("modbus_connection_manager.set_credential") as span:
            span.set_attribute("connection_id", connection_id)
            record = self.connections.get(connection_id)
            if record is None:
                span.set_attribute("found", False)
                return None
            record["credential"] = credential
            self._save()
            span.set_attribute("found", True)
            return record

    async def test_read(self, host, register, port=502, device_id=1, point=None) -> dict:
        """Read one point from a device that isn't a connection yet - the
        Connectors page's "Test read" before saving (Baler#110). `point` is
        an optional stored-form spec (type, function, byte order...), as in
        a connection's points; without one, `register` as a holding
        register. A fresh client, closed again whatever happens; an
        unreachable host or a refused read is a ValueError saying so."""
        with tracer.start_as_current_span("modbus_connection_manager.test_read") as span:
            host = connection_store.check_text("host", host).strip()
            port = connection_store.check_int("port", port, 1, 65535)
            device_id = connection_store.check_int("device_id", device_id, 0, 255)
            register = connection_store.check_int("register", register, 0, 65535)
            spec = {**point, "register": register} if isinstance(point, dict) else register
            resolved = modbus_points.resolve(spec)
            span.set_attribute("host", host)
            span.set_attribute("register", register)
            where = f"{host}:{port}"
            client = self._new_client({"host": host, "port": port})
            try:
                try:
                    connected = await asyncio.wait_for(client.connect(), TEST_READ_TIMEOUT)
                except (asyncio.TimeoutError, OSError) as exc:
                    raise ValueError(f"can't connect to {where}: {exc or 'timed out'}") from exc
                if not connected:
                    raise ValueError(f"can't connect to {where}: nothing accepted the connection")
                try:
                    value = await asyncio.wait_for(
                        self._read(client, {"device_id": device_id}, resolved), TEST_READ_TIMEOUT
                    )
                except asyncio.TimeoutError as exc:
                    raise ValueError(f"{where} didn't answer a read of register {register} (unit {device_id})") from exc
                except Exception as exc:  # pymodbus raises many types; all mean "read failed"
                    raise ValueError(f"reading register {register} from {where} failed: {exc}") from exc
                if value is None:
                    raise ValueError(f"{where} refused register {register} (unit {device_id}) or sent no usable value")
                return {"host": host, "port": port, "device_id": device_id, "register": register, "value": value}
            finally:
                client.close()

    async def close(self) -> None:
        """Cancels every poll task and closes every client without
        touching persisted state - process-shutdown cleanup (api.py's
        async_main finally block), distinct from delete_connection which
        also removes the connection from the state file. Matters in
        practice, not just in theory: an unclosed AsyncModbusTcpClient
        leaves its own internal reconnect task alive, which can stall
        asyncio's default task-cancellation-on-shutdown indefinitely -
        confirmed live, not guessed."""
        with tracer.start_as_current_span("modbus_connection_manager.close") as span:
            for task in self.tasks.values():
                task.cancel()
            for client in self.clients.values():
                client.close()
            span.set_attribute("closed_count", len(self.clients))
            self.tasks.clear()
            self.clients.clear()

    def _save(self) -> None:
        connection_store.save(self.state_file, list(self.connections.values()))

    async def _start_poll_task(self, record: dict) -> None:
        """Never connects here: the poll loop owns the client, so a device
        that isn't listening yet (or is rebooting) is retried by the loop
        instead of leaving a dead client behind (Baler#83)."""
        task = asyncio.get_event_loop().create_task(self._poll_loop(record))
        task.add_done_callback(self._make_crash_logger(record["id"]))
        self.tasks[record["id"]] = task

    def _make_crash_logger(self, connection_id: str):
        def on_done(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is None:
                return
            print(f"modbus connection {connection_id} poll task crashed: {exc!r}")
            with tracer.start_as_current_span("modbus_connection_manager.poll_task_crashed") as span:
                span.set_attribute("connection_id", connection_id)
                span.set_attribute("error", str(exc))

        return on_done

    def _new_client(self, record: dict) -> ModbusClient.AsyncModbusTcpClient:
        # reconnect_delay=0 turns pymodbus's own background reconnect off:
        # the poll loop reconnects on its own schedule, and the library's
        # task never retried a first connect that failed (Baler#83).
        return ModbusClient.AsyncModbusTcpClient(record["host"], port=record["port"], reconnect_delay=0)

    async def _connect(self, record: dict) -> ModbusClient.AsyncModbusTcpClient | None:
        old = self.clients.pop(record["id"], None)
        if old is not None:
            old.close()
        client = self._new_client(record)
        self.clients[record["id"]] = client
        try:
            if await client.connect():
                return client
        except Exception as exc:  # noqa: BLE001 - any connect failure means "retry later"
            print(f"modbus connect to {record['host']}:{record['port']} raised {exc!r}")
        return None

    async def _poll_loop(self, record: dict) -> None:
        """Read every point, publish what was read, sleep, repeat.

        A failed read publishes nothing - a null reading made a dead
        controller look alive downstream (Baler#83/#85). While the device
        is unreachable the loop reconnects with backoff, from one poll
        interval up to RECONNECT_MAX seconds, and resumes on its own when
        the device comes back. A cycle where every read fails on a client
        that still claims to be connected (a half-open socket) forces a
        fresh connection too."""
        where = f"{record['host']}:{record['port']}"
        client = None
        backoff = record["poll_interval"]
        down_since: float | None = None
        attempts = 0
        while True:
            if client is None or not client.connected:
                client = await self._connect(record)
                if client is None:
                    attempts += 1
                    self.status.failure(record.get("id"), list(record["points"]), f"can't connect to {where}")
                    if down_since is None:
                        down_since = asyncio.get_event_loop().time()
                    if attempts in (1, 5) or attempts % 50 == 0:
                        print(f"modbus device {where} unreachable ({attempts} attempts); retrying in {backoff:g}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max(RECONNECT_MAX, record["poll_interval"]))
                    continue
                if down_since is not None:
                    print(f"modbus device {where} reconnected after {attempts} attempts")
                down_since, attempts, backoff = None, 0, record["poll_interval"]

            raised = 0
            async with self._lock(record["id"]):
                for label, spec in record["points"].items():
                    point = modbus_points.resolve(spec)
                    try:
                        value = await self._read(client, record, point)
                    except Exception as exc:  # noqa: BLE001 - a lost session raises many types; all mean reconnect
                        raised += 1
                        print(f"modbus read failed for {label}@{point['register']}: {exc!r}")
                        self.status.failure(record.get("id"), label, f"register {point['register']}: {exc}")
                        continue
                    if value is None:  # an exception code (or NaN): the session is fine, publish nothing
                        self.status.failure(record.get("id"), label,
                                            f"register {point['register']}: the device refused it or sent no usable value")
                        continue
                    mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], label, value)
                    self.status.success(record.get("id"), label, published=True)
            if record["points"] and raised == len(record["points"]):
                client.close()  # every read raised: next cycle reconnects
            await asyncio.sleep(record["poll_interval"])
