"""
Modbus counterpart to connection_manager.py - deliberately smaller, not full
parity. Modbus has no shared Application (one AsyncModbusTcpClient per
connection, same as modbus_bridge.py already does) and no learn() (Modbus
has no self-describing object-list the way BACnet's object-list property
does - register addresses are opaque and vendor-documented, so a human
supplies {label: address} points at connection-creation time, same shape
modbus_bridge.py's --points flag already produces via parse_points()).

One poll task per *connection*, not per point: modbus_bridge.poll_forever's
loop-over-all-points-per-interval body, adapted to a configurable interval
and made cancellable, rather than reproducing BACnet's per-point-task/COV
machinery - nothing here asked for per-point poll intervals on Modbus, and
building that would be over-parity for no requirement driving it.
"""

import asyncio
import contextlib

import pymodbus.client as ModbusClient

from windrow import connection_store, mqtt_sink, tracing
from windrow.modbus_bridge import normalize_value

tracer = tracing.get_tracer(__name__)

# Longest wait between reconnect attempts to an unreachable device.
RECONNECT_MAX = 60.0

UPDATABLE_FIELDS = {"points", "poll_interval", "topic_prefix", "host", "port", "device_id"}


def validate_points(points) -> dict[str, int]:
    """{label: holding-register address}, non-empty - the same shape
    create_connection stores and modbus_bridge.parse_points produces."""
    if not isinstance(points, dict) or not points:
        raise ValueError("points must be a non-empty {label: register_address} object")
    for label, address in points.items():
        connection_store.check_text("point label", label)
        connection_store.check_int(f"register address for {label!r}", address, 0, 65535)
    return dict(points)


class ModbusConnectionManager:
    def __init__(self, mqtt_client, state_file: str):
        self.mqtt_client = mqtt_client
        self.state_file = state_file
        self.connections: dict[str, dict] = {}
        self.clients: dict[str, ModbusClient.AsyncModbusTcpClient] = {}
        self.tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        with tracer.start_as_current_span("modbus_connection_manager.start") as span:
            records = connection_store.load(self.state_file)
            for record in records:
                self.connections[record["id"]] = record
                await self._start_poll_task(record)
            span.set_attribute("restored_count", len(records))

    async def create_connection(
        self,
        host: str,
        topic_prefix: str,
        points: dict[str, int],
        port: int = 502,
        device_id: int = 1,
        poll_interval: float = 5.0,
        credential: dict | None = None,
    ) -> dict:
        with tracer.start_as_current_span("modbus_connection_manager.create_connection") as span:
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
            self.connections[record["id"]] = record
            self._save()
            await self._start_poll_task(record)
            span.set_attribute("connection_id", record["id"])
            span.set_attribute("point_count", len(points))
            return record

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
        topics cleared. Modbus connections publish no tags of their own;
        clearing `/tags` too catches any a human added to the point."""
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

            task = self.tasks.pop(connection_id, None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            client = self.clients.pop(connection_id, None)
            if client is not None:
                client.close()
            self.connections[connection_id] = record
            self._save()

            if record["topic_prefix"] != old["topic_prefix"]:
                removed = list(old["points"])
            else:
                removed = [label for label in old["points"] if label not in record["points"]]
            for label in removed:
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], label)
                mqtt_sink.clear_retained(self.mqtt_client, old["topic_prefix"], f"{label}/tags")

            await self._start_poll_task(record)
            span.set_attribute("found", True)
            span.set_attribute("cleared_count", len(removed))
            return record

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
            for label, address in record["points"].items():
                try:
                    rr = await client.read_holding_registers(address, count=1, device_id=record["device_id"])
                except Exception as exc:  # noqa: BLE001 - a lost session raises many types; all mean reconnect
                    raised += 1
                    print(f"modbus read failed for {label}@{address}: {exc!r}")
                    continue
                if rr.isError():  # the device answered with an exception code: the session is fine
                    print(f"modbus read error for {label}@{address}: {rr}")
                    continue
                mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], label, normalize_value(rr.registers))
            if record["points"] and raised == len(record["points"]):
                client.close()  # every read raised: next cycle reconnects
            await asyncio.sleep(record["poll_interval"])
