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

import pymodbus.client as ModbusClient

from windrow import connection_store, mqtt_sink, tracing
from windrow.modbus_bridge import normalize_value

tracer = tracing.get_tracer(__name__)


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
        client = ModbusClient.AsyncModbusTcpClient(record["host"], port=record["port"])
        await client.connect()
        self.clients[record["id"]] = client
        task = asyncio.get_event_loop().create_task(self._poll_loop(record, client))
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

    async def _poll_loop(self, record: dict, client: ModbusClient.AsyncModbusTcpClient) -> None:
        while True:
            for label, address in record["points"].items():
                try:
                    rr = await client.read_holding_registers(address, count=1, device_id=record["device_id"])
                    value = None if rr.isError() else normalize_value(rr.registers)
                except Exception as exc:
                    value = None
                    print(f"modbus read failed for {label}@{address}: {exc!r}")
                mqtt_sink.publish_reading(self.mqtt_client, record["topic_prefix"], label, value)
            await asyncio.sleep(record["poll_interval"])
