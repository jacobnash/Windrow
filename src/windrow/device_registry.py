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
    bacnet_network,
    connection_store,
    default_credential_check,
    discovery,
    modbus_discovery,
    tracing,
)

tracer = tracing.get_tracer(__name__)

# Per directed (unicast) Who-Is: an answering device replies in well under
# a second; a dead one shouldn't hold up the pass for long.
DIRECTED_WHO_IS_TIMEOUT = 3.0

DEVICE_STATUSES = ("pending", "provisioned", "ignored")
BACNET_PORT = 47808
MAX_BACNET_INSTANCE = 4194302  # 2^22 - 2; 4194303 is the "unconfigured" wildcard


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
        if status not in DEVICE_STATUSES:
            raise ValueError(f"status must be one of {', '.join(DEVICE_STATUSES)}, got {status!r}")
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

    async def add_device(self, body: dict, connections: list[dict] | None = None) -> dict:
        """A device a human already knows the address of, for the sites
        where a scan can't find it (broadcasts filtered between subnets,
        Modbus on a subnet nobody configured a sweep for). Creates the same
        record reconcile_bacnet/reconcile_modbus would - keyed the same way,
        so a later scan that does hear the device updates this record
        instead of adding a twin - or returns the record that already
        exists. Status is pending unless one of `connections` (that
        protocol's) already polls the device, matching reconcile.

        body: {"protocol": "bacnet", "device_address", "device_instance"}
        or {"protocol": "modbus", "host", "port" (502), "device_id" (1)}.
        Raises ValueError on anything else."""
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")  # noqa: TRY004 - ValueError means 400 / ok:false
        protocol = body.get("protocol")
        if protocol == "bacnet":
            return self._add_bacnet(body, connections or [])
        if protocol == "modbus":
            return self._add_modbus(body, connections or [])
        raise ValueError(f"protocol must be 'bacnet' or 'modbus', got {protocol!r}")

    def _add_bacnet(self, body: dict, connections: list[dict]) -> dict:
        address = connection_store.check_text("device_address", body.get("device_address")).strip()
        bacnet_network.parse_device_address(address)  # a routed net:mac is fine; a broadcast or garbage isn't
        instance = connection_store.check_int("device_instance", body.get("device_instance"), 0, MAX_BACNET_INSTANCE)
        # Discovery reports a bare host when the device answers on the
        # standard port; keep a non-standard one, since that's the only
        # way "Create connection" can reach the device later.
        host, _, port = address.partition(":")
        if port == str(BACNET_PORT) and "." in host:  # not a routed net:mac
            address = host
        existing = self._find("bacnet", (instance, address))
        if existing is not None:
            return existing
        connection_id = next(
            (c["id"] for c in connections
             if c.get("device_instance") == instance
             and bacnet_network.address_key(c.get("device_address", "")) == bacnet_network.address_key(address)),
            None,
        )
        now = time.time()
        record = {
            "id": connection_store.new_connection_id(),
            "protocol": "bacnet",
            "device_instance": instance,
            "address": address,
            "vendor_id": None,  # unknown until a scan hears its I-Am
            "first_seen_at": now,
            "last_seen_at": now,
            "status": "provisioned" if connection_id else "pending",
            "connection_id": connection_id,
            "credential": None,
            "default_credential_flag": None,
            "default_credential_checked_at": None,
        }
        self.records[record["id"]] = record
        self._save()
        return record

    def _add_modbus(self, body: dict, connections: list[dict]) -> dict:
        host = connection_store.check_text("host", body.get("host")).strip()
        port = connection_store.check_int("port", body.get("port", modbus_discovery.DEFAULT_PORT), 1, 65535)
        device_id = connection_store.check_int("device_id", body.get("device_id", 1), 0, 255)
        existing = self._find("modbus", (host, port))
        if existing is not None:
            return existing
        connection_id = next((c["id"] for c in connections if (c.get("host"), c.get("port")) == (host, port)), None)
        now = time.time()
        record = {
            "id": connection_store.new_connection_id(),
            "protocol": "modbus",
            "host": host,
            "port": port,
            # Not in a scanned record (a sweep can't learn the unit id);
            # kept so "Create connection" can prefill what the human typed.
            "device_id": device_id,
            "vendor": None,
            "product_code": None,
            "revision": None,
            "first_seen_at": now,
            "last_seen_at": now,
            "status": "provisioned" if connection_id else "pending",
            "connection_id": connection_id,
            "credential": None,
            "default_credential_flag": None,
            "default_credential_checked_at": None,
        }
        self.records[record["id"]] = record
        self._save()
        if connection_id is None:
            # Same one-time default-credential check a newly scanned
            # pending Modbus device gets (see reconcile_modbus).
            asyncio.get_event_loop().create_task(self._run_default_check(record["id"], host))
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
            # here so both sides of the key compare on host alone. A routed
            # `net:mac` address is kept whole (bacnet_network.address_key).
            connection_by_key = {
                (c["device_instance"], bacnet_network.address_key(c["device_address"])): c["id"] for c in connections
            }
            now = time.time()
            for device in devices:
                key = (device["device_instance"], bacnet_network.address_key(device["address"]))
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
                if record["protocol"] == "bacnet" and (
                    (record["device_instance"], bacnet_network.address_key(record["address"]))
                    == (key[0], bacnet_network.address_key(key[1]))
                ):
                    return record
        else:
            for record in self.records.values():
                if record["protocol"] == "modbus" and (record["host"], record["port"]) == key:
                    return record
        return None

    def _save(self) -> None:
        connection_store.save(self.state_file, list(self.records.values()))

    # --- periodic scan loops ---

    async def scan_bacnet(self, app, connections: list[dict]) -> list[dict]:
        """One discovery pass: a broadcast Who-Is, then a directed Who-Is to
        each known connection's address the broadcast didn't answer for.

        A broadcast alone misses devices it can't reach (Baler#52): it goes
        to *this* app's own UDP port, so a device on another port never
        hears it (the demo: Windrow on :47820, the mock device on :47808),
        and real sites often filter broadcasts between subnets (VLANs, no
        BBMD). A unicast Who-Is reaches whatever polling already reaches.
        """
        found: list[dict] = []
        try:
            found = await discovery.discover_devices(app)
        except Exception as exc:  # noqa: BLE001 - try the directed probes anyway
            print(f"bacnet scan: broadcast Who-Is failed: {exc!r}")
        heard = {bacnet_network.address_key(d["address"]) for d in found}
        targets = sorted({c["device_address"] for c in connections
                          if c.get("device_address") and bacnet_network.address_key(c["device_address"]) not in heard})
        directed = 0
        for address in targets:
            try:
                replies = await discovery.discover_devices(app, address=address, timeout=DIRECTED_WHO_IS_TIMEOUT)
            except Exception as exc:  # noqa: BLE001 - one unreachable device mustn't stop the rest
                print(f"bacnet scan: directed Who-Is to {address} failed: {exc!r}")
                continue
            known = {(x["device_instance"], x["address"]) for x in found}
            fresh = [d for d in replies if (d["device_instance"], d["address"]) not in known]
            found += fresh
            directed += len(fresh)
        print(f"bacnet scan: {len(found)} device(s) ({directed} via directed Who-Is to {len(targets)} known address(es))")
        return found

    async def run_periodic_bacnet_scan(self, app, connection_manager, interval: float) -> None:
        while True:
            connections = list(connection_manager.connections.values())
            devices = await self.scan_bacnet(app, connections)
            await self.reconcile_bacnet(devices, connections)
            await asyncio.sleep(interval)

    async def run_periodic_modbus_scan(self, cidr: str, connection_manager, interval: float) -> None:
        while True:
            devices = await modbus_discovery.sweep(cidr)
            await self.reconcile_modbus(devices, list(connection_manager.connections.values()))
            await asyncio.sleep(interval)
