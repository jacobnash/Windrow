"""
Tracks devices found by discovery, independent of connection_store.json /
windrow-modbus-connections.json: a discovered device has no id/topic_prefix/
points until a human provisions it, and ConnectionManager.start() already
trusts every record in its own state file as "actively poll this" - that
invariant stays untouched by keeping discovery bookkeeping in its own file
(windrow-devices.json, via the same generic connection_store.load/save).

Owns the periodic re-scan loops (BACnet Who-Is on a timer, Modbus CIDR
sweeps on a timer) and reconciles their results against this registry plus
whichever devices are already provisioned as real connections.
"""

# Needed for the type hints below to work at all - `def list(self) -> ...`
# shadows the builtin `list` for every annotation later in this class body
# (Python evaluates them eagerly at class-definition time otherwise), so
# reconcile_bacnet's `devices: list[dict]` was crashing with
# "'function' object is not subscriptable" on import, before this. This
# makes annotations lazy strings instead, sidestepping the shadowing
# without renaming the public list() method.
from __future__ import annotations

import asyncio
import time

from windrow import (
    connection_store,
    default_credential_check,
    discovery,
    modbus_discovery,
    tracing,
)

tracer = tracing.get_tracer(__name__)


class DeviceRegistry:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.records: dict[str, dict] = {}

    async def start(self) -> None:
        with tracer.start_as_current_span("device_registry.start") as span:
            records = connection_store.load(self.state_file)
            self.records = {r["id"]: r for r in records}
            span.set_attribute("restored_count", len(self.records))

    def list(self) -> list[dict]:
        return list(self.records.values())

    def get(self, device_id: str) -> dict | None:
        return self.records.get(device_id)

    async def set_status(self, device_id: str, status: str) -> dict | None:
        """async for consistency with the rest of this app's invariant
        (loop-thread-owned state mutated only via run_coroutine_threadsafe
        from api.py) even though the body itself doesn't await anything."""
        record = self.records.get(device_id)
        if record is None:
            return None
        record["status"] = status
        self._save()
        return record

    async def set_credential(self, device_id: str, credential: dict) -> dict | None:
        record = self.records.get(device_id)
        if record is None:
            return None
        record["credential"] = credential
        self._save()
        return record

    async def recheck_credentials(self, device_id: str) -> dict | None:
        """Manual re-trigger (e.g. after a human changes a device's
        password and wants to confirm the flag clears) - a no-op for
        BACnet, which never gets a default-credential check (see
        default_credential_check.py's module docstring for why)."""
        record = self.records.get(device_id)
        if record is None or record["protocol"] != "modbus":
            return record
        result = await asyncio.to_thread(default_credential_check.check, record["host"])
        record["default_credential_flag"] = result
        record["default_credential_checked_at"] = time.time()
        self._save()
        return record

    # --- BACnet ---

    async def reconcile_bacnet(self, devices: list[dict], connections: list[dict]) -> None:
        with tracer.start_as_current_span("device_registry.reconcile_bacnet") as span:
            # discover_devices()'s own "address" field never carries a port
            # ("no port - the standard BACnet/IP port 47808 is implied",
            # per openapi.yaml's Device schema), but a connection's
            # device_address is whatever a human typed when creating it and
            # commonly does include one (e.g. "192.168.128.63:47808",
            # matching every worked example in this codebase) - stripped
            # here so both sides of the key compare on host alone.
            connection_by_key = {
                (c["device_instance"], c["device_address"].split(":")[0]): c["id"] for c in connections
            }
            now = time.time()
            for device in devices:
                key = (device["device_instance"], device["address"])
                existing = self._find("bacnet", key)
                connection_id = connection_by_key.get(key)

                if existing is None:
                    record = {
                        "id": connection_store.new_connection_id(),
                        "protocol": "bacnet",
                        "device_instance": device["device_instance"],
                        "address": device["address"],
                        "vendor_id": device["vendor_id"],
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "status": "provisioned" if connection_id else "pending",
                        "connection_id": connection_id,
                        "credential": None,
                        # BACnet has no login surface at the protocol level - always null,
                        # not silently skipped. See default_credential_check.py.
                        "default_credential_flag": None,
                        "default_credential_checked_at": None,
                    }
                    self.records[record["id"]] = record
                else:
                    existing["last_seen_at"] = now
                    existing["vendor_id"] = device["vendor_id"]
                    if connection_id:
                        existing["status"] = "provisioned"
                        existing["connection_id"] = connection_id
                    elif existing["status"] == "provisioned":
                        existing["status"] = "pending"
                        existing["connection_id"] = None
            self._save()
            span.set_attribute("device_count", len(devices))

    # --- Modbus ---

    async def reconcile_modbus(self, devices: list[dict], connections: list[dict]) -> None:
        with tracer.start_as_current_span("device_registry.reconcile_modbus") as span:
            connection_by_key = {(c["host"], c["port"]): c["id"] for c in connections}
            now = time.time()
            new_pending_hosts = []
            for device in devices:
                key = (device["host"], device["port"])
                existing = self._find("modbus", key)
                connection_id = connection_by_key.get(key)

                if existing is None:
                    status = "provisioned" if connection_id else "pending"
                    record = {
                        "id": connection_store.new_connection_id(),
                        "protocol": "modbus",
                        "host": device["host"],
                        "port": device["port"],
                        "vendor": device.get("vendor"),
                        "product_code": device.get("product_code"),
                        "revision": device.get("revision"),
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "status": status,
                        "connection_id": connection_id,
                        "credential": None,
                        "default_credential_flag": None,
                        "default_credential_checked_at": None,
                    }
                    self.records[record["id"]] = record
                    if status == "pending":
                        new_pending_hosts.append((record["id"], device["host"]))
                else:
                    existing["last_seen_at"] = now
                    existing["vendor"] = device.get("vendor")
                    existing["product_code"] = device.get("product_code")
                    existing["revision"] = device.get("revision")
                    if connection_id:
                        existing["status"] = "provisioned"
                        existing["connection_id"] = connection_id
                    elif existing["status"] == "provisioned":
                        existing["status"] = "pending"
                        existing["connection_id"] = None
            self._save()
            span.set_attribute("device_count", len(devices))

            # Fire-and-forget, only for a device seen as pending for the
            # first time - not re-run on every periodic pass against
            # already-known devices, or this would hammer login pages
            # forever for no reason.
            for record_id, host in new_pending_hosts:
                asyncio.get_event_loop().create_task(self._run_default_check(record_id, host))

    async def _run_default_check(self, record_id: str, host: str) -> None:
        result = await asyncio.to_thread(default_credential_check.check, host)
        record = self.records.get(record_id)
        if record is None:
            return
        record["default_credential_flag"] = result
        record["default_credential_checked_at"] = time.time()
        self._save()

    def _find(self, protocol: str, key: tuple) -> dict | None:
        if protocol == "bacnet":
            for record in self.records.values():
                if record["protocol"] == "bacnet" and (record["device_instance"], record["address"]) == key:
                    return record
        else:
            for record in self.records.values():
                if record["protocol"] == "modbus" and (record["host"], record["port"]) == key:
                    return record
        return None

    def _save(self) -> None:
        connection_store.save(self.state_file, list(self.records.values()))

    # --- periodic scan loops ---

    async def run_periodic_bacnet_scan(self, app, connection_manager, interval: float) -> None:
        while True:
            devices = await discovery.discover_devices(app)
            await self.reconcile_bacnet(devices, list(connection_manager.connections.values()))
            await asyncio.sleep(interval)

    async def run_periodic_modbus_scan(self, cidr: str, connection_manager, interval: float) -> None:
        while True:
            devices = await modbus_discovery.sweep(cidr)
            await self.reconcile_modbus(devices, list(connection_manager.connections.values()))
            await asyncio.sleep(interval)
