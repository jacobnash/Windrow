"""
Device announce: "I am here" from the LAN, so connections follow a device
when its address changes.

Field devices sit on DHCP or get re-addressed; a connection pinned to an IP
then polls nothing until someone notices. Two ways to keep it pointed right:

  1. Announce. A small sender on the device (or on a Pi next to it)
     - `python -m windrow.announce` - sends a UDP datagram to its Windrow
     every --interval seconds, and at once when its own address changes:

         {"v": 1, "name": "bacnet", "fqdn": "bacnet.lan", "ips": [...],
          "services": [{"kind": "bacnet", "port": 47808, "instance": 10228}],
          "ts": "...", "sig": "<hex hmac>"}

     Windrow's AnnounceRegistry takes the device's address from the
     datagram's *source IP*, never from the payload (the payload's `ips`
     are informational), refuses sources outside --announce-allow, and,
     when WINDROW_ANNOUNCE_KEY is set on both ends, refuses anything not
     signed with it (HMAC-SHA256 over the payload without `sig`) or older
     than MAX_SKEW. No broker account is needed on the device.

  2. DNS. A connection names its device (`device_name`); if nothing has
     announced under that name recently, Windrow resolves it as a host name
     every --resolve-interval seconds (and right after a failed poll would
     be the next step - not done yet).

Either way, when the address behind a connection's device_name differs
from the one it polls, the connection is re-pointed through its manager's
update_connection - the same validate/persist/restart path an edit takes
- and the change is logged, kept in the registry's history and shown in
the status heartbeat.

Pure parsing/verification lives in functions so tests need no sockets.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import ipaddress
import json
import os
import socket
import time
from datetime import datetime, timezone

from windrow import connection_store, tracing

tracer = tracing.get_tracer(__name__)

DEFAULT_PORT = 47900
PROTOCOL_VERSION = 1
MAX_DATAGRAM = 4096
MAX_SKEW = 300.0  # seconds a signed announcement may be off from our clock
STALE_AFTER = 3  # announcements missed before a device counts as offline
MAX_HISTORY = 20
KINDS = ("bacnet", "modbus", "other")


def _now() -> float:
    return time.time()


def _iso(ts: float | None) -> str | None:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None


def _canonical(payload: dict) -> bytes:
    body = {k: v for k, v in payload.items() if k != "sig"}
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def sign(payload: dict, key: str) -> dict:
    return {**payload, "sig": hmac.new(key.encode(), _canonical(payload), hashlib.sha256).hexdigest()}


def verify(payload: dict, key: str) -> bool:
    sig = payload.get("sig")
    if not isinstance(sig, str):
        return False
    want = hmac.new(key.encode(), _canonical(payload), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, want)


# ------------------------------------------------------------- sender side


def parse_service(text: str) -> dict:
    """`bacnet:47808:10228` (port, device instance), `modbus:502:1` (port,
    unit id) or `other:<port>`."""
    parts = text.split(":")
    kind = parts[0]
    if kind not in KINDS or len(parts) < 2:
        raise ValueError(f"service {text!r}: kind:port[:id] with kind one of {', '.join(KINDS)}")
    out = {"kind": kind, "port": int(parts[1])}
    if kind == "bacnet" and len(parts) > 2:
        out["instance"] = int(parts[2])
    if kind == "modbus" and len(parts) > 2:
        out["unit"] = int(parts[2])
    return out


def source_ip_towards(host: str, port: int = DEFAULT_PORT) -> str | None:
    """The local address the OS would send from to reach `host` - the one
    that matters to the receiver. A UDP connect sends nothing."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((host, port))
            return s.getsockname()[0]
    except OSError:
        return None


def build_announcement(name: str, services: list[dict], ips: list[str], key: str | None = None,
                       fqdn: str | None = None, now: float | None = None) -> dict:
    payload = {"v": PROTOCOL_VERSION, "name": name, "fqdn": fqdn or socket.getfqdn(), "ips": ips,
               "services": services, "ts": _iso(now or _now())}
    return sign(payload, key) if key else payload


async def run_sender(name: str, targets: list[tuple[str, int]], services: list[dict], interval: float,
                     key: str | None, check_every: float = 5.0) -> None:
    """Announce every `interval`, and straight away when the address we'd
    send from changes (DHCP renew, cable moved, new subnet)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    last_ips: list[str] | None = None
    last_sent = 0.0
    while True:
        ips = sorted({ip for host, port in targets if (ip := source_ip_towards(host, port))})
        if ips != last_ips or _now() - last_sent >= interval:
            if last_ips is not None and ips != last_ips:
                print(f"address changed {last_ips} -> {ips}: announcing now")
            data = json.dumps(build_announcement(name, services, ips, key)).encode()
            for host, port in targets:
                try:
                    sock.sendto(data, (host, port))
                except OSError as exc:
                    print(f"announce to {host}:{port} failed: {exc!r}")
            last_ips, last_sent = ips, _now()
        await asyncio.sleep(check_every)


def _target(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return (host, int(port)) if host else (text, DEFAULT_PORT)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Announce this device to its Windrow: 'I am here'.")
    p.add_argument("--name", default=os.environ.get("WINDROW_ANNOUNCE_NAME", socket.gethostname()),
                   help="the name connections use (device_name); default: this host's name")
    p.add_argument("--to", action="append", default=[],
                   help="Windrow host[:port] to announce to (repeatable; a broadcast address works on the LAN). "
                        "Default: $WINDROW_ANNOUNCE_TO, comma separated")
    p.add_argument("--service", action="append", default=[], help="kind:port[:id], e.g. bacnet:47808:10228, modbus:502:1")
    p.add_argument("--interval", type=float, default=30.0)
    args = p.parse_args(argv)
    targets = [_target(t) for t in (args.to or [x for x in os.environ.get("WINDROW_ANNOUNCE_TO", "").split(",") if x])]
    if not targets:
        p.error("no --to (or WINDROW_ANNOUNCE_TO) given")
    services = [parse_service(s) for s in args.service]
    key = os.environ.get("WINDROW_ANNOUNCE_KEY") or None
    print(f"announcing {args.name!r} {services} to {targets} every {args.interval:g}s ({'signed' if key else 'unsigned'})")
    asyncio.run(run_sender(args.name, targets, services, args.interval, key))


# ----------------------------------------------------------- receiver side


class Rejected(ValueError):
    pass


def check_announcement(raw: bytes, source_ip: str, allow: list, key: str | None, now: float) -> dict:
    """Parse and vet one datagram. Returns {name, address, services, ...};
    raises Rejected with the reason."""
    if len(raw) > MAX_DATAGRAM:
        raise Rejected("datagram too large")
    try:
        ip = ipaddress.ip_address(source_ip)
    except ValueError as exc:
        raise Rejected(f"bad source {source_ip!r}") from exc
    if allow and not any(ip in net for net in allow):
        raise Rejected(f"source {source_ip} is outside --announce-allow")
    try:
        payload = json.loads(raw.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Rejected("not JSON") from exc
    if not isinstance(payload, dict) or payload.get("v") != PROTOCOL_VERSION:
        raise Rejected("not a v1 announcement")
    name = payload.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 253:
        raise Rejected("missing name")
    if key:
        if not verify(payload, key):
            raise Rejected(f"{name}: bad or missing signature")
        try:
            sent = datetime.fromisoformat(str(payload.get("ts"))).timestamp()
        except ValueError as exc:
            raise Rejected(f"{name}: bad ts") from exc
        if abs(now - sent) > MAX_SKEW:
            raise Rejected(f"{name}: ts is {now - sent:+.0f}s off (replayed, or a clock is wrong)")
    services = [s for s in payload.get("services") or [] if isinstance(s, dict) and s.get("kind") in KINDS]
    return {"name": name.strip().lower(), "address": str(ip), "fqdn": payload.get("fqdn"),
            "ips": payload.get("ips") or [], "services": services}


def _host_of(device_address: str) -> str:
    """BACnet device_address is `ip`, `ip:port` or `ip/prefix:port`."""
    host = device_address.rsplit(":", 1)[0] if device_address.count(":") == 1 else device_address
    return host.split("/", 1)[0]


def _with_host(device_address: str, host: str) -> str:
    """Keep the port (and prefix length) of a BACnet address, swap the IP."""
    rest = device_address[len(_host_of(device_address)):]
    return host + rest


class AnnounceRegistry:
    """Devices that said "I am here", by name, persisted like connections.
    `repoint` is called (from the asyncio loop) with (name, old, new) when
    a device's address changes."""

    def __init__(self, state_file: str, interval: float = 30.0, allow: list | None = None,
                 key: str | None = None, clock=_now):
        self.state_file = state_file
        self.interval = interval
        self.allow = allow or []
        self.key = key
        self.clock = clock
        self.devices: dict[str, dict] = {}
        self.rejected = 0
        self.last_rejection: str | None = None

    def load(self) -> None:
        self.devices = {d["name"]: d for d in connection_store.load(self.state_file)}

    def _save(self) -> None:
        connection_store.save(self.state_file, list(self.devices.values()))

    def online(self, name: str) -> bool:
        d = self.devices.get(name)
        return bool(d) and self.clock() - d["last_seen"] <= self.interval * STALE_AFTER

    def address(self, name: str) -> str | None:
        """The announced address, only while the device is still announcing."""
        return self.devices[name]["address"] if self.online(name) else None

    def receive(self, raw: bytes, source_ip: str) -> tuple[dict, str | None] | None:
        """One datagram -> (device record, previous address if it moved),
        or None when rejected."""
        with tracer.start_as_current_span("announce.receive") as span:
            span.set_attribute("source", source_ip)
            now = self.clock()
            try:
                a = check_announcement(raw, source_ip, self.allow, self.key, now)
            except Rejected as exc:
                self.rejected += 1
                self.last_rejection = f"{_iso(now)} {source_ip}: {exc}"
                span.set_attribute("rejected", str(exc))
                return None
            old = self.devices.get(a["name"])
            moved = old["address"] if old and old["address"] != a["address"] else None
            history = list((old or {}).get("history") or [])
            if moved:
                history = (history + [{"at": _iso(now), "from": moved, "to": a["address"]}])[-MAX_HISTORY:]
            rec = {**a, "first_seen": (old or {}).get("first_seen", now), "last_seen": now, "history": history}
            self.devices[a["name"]] = rec
            if moved or old is None or old.get("services") != rec["services"]:
                self._save()
            span.set_attribute("name", a["name"])
            span.set_attribute("moved", bool(moved))
            return rec, moved

    def list(self) -> list[dict]:
        return [{"name": d["name"], "address": d["address"], "fqdn": d.get("fqdn"), "services": d.get("services", []),
                 "online": self.online(d["name"]), "last_seen": _iso(d["last_seen"]),
                 "first_seen": _iso(d.get("first_seen")), "history": d.get("history", [])}
                for d in sorted(self.devices.values(), key=lambda d: d["name"])]

    def summary(self) -> dict:
        devs = self.list()
        return {"announced": len(devs), "announced_online": sum(1 for d in devs if d["online"]),
                "announce_rejected": self.rejected}


def resolve(name: str) -> str | None:
    try:
        infos = socket.getaddrinfo(name, None, socket.AF_INET)
    except OSError:
        return None
    return infos[0][4][0] if infos else None


async def follow(name: str, address: str, manager, modbus_manager) -> list[dict]:
    """Re-point every connection whose device_name is `name` and that polls
    somewhere else. Returns what changed."""
    changed = []
    for rec in list(manager.connections.values()):
        if (rec.get("device_name") or "").lower() == name and _host_of(rec["device_address"]) != address:
            new = _with_host(rec["device_address"], address)
            await manager.update_connection(rec["id"], {"device_address": new})
            changed.append({"kind": "bacnet", "id": rec["id"], "from": rec["device_address"], "to": new})
    for rec in list(modbus_manager.connections.values()):
        if (rec.get("device_name") or "").lower() == name and rec["host"] != address:
            await modbus_manager.update_connection(rec["id"], {"host": address})
            changed.append({"kind": "modbus", "id": rec["id"], "from": rec["host"], "to": address})
    for c in changed:
        print(f"device {name!r} moved: {c['kind']} connection {c['id']} re-pointed {c['from']} -> {c['to']}")
    return changed


def named_devices(manager, modbus_manager) -> set[str]:
    names = {(r.get("device_name") or "").lower() for r in manager.connections.values()}
    names |= {(r.get("device_name") or "").lower() for r in modbus_manager.connections.values()}
    return {n for n in names if n}


async def run_resolver(registry: AnnounceRegistry, manager, modbus_manager, interval: float, resolver=resolve) -> None:
    """For named connections whose device isn't announcing: follow DNS.
    An announcing device always wins over DNS."""
    loop = asyncio.get_running_loop()
    while True:
        for name in sorted(named_devices(manager, modbus_manager)):
            if registry.address(name):
                address = registry.address(name)
            else:
                address = await loop.run_in_executor(None, resolver, name)
            if address:
                try:
                    await follow(name, address, manager, modbus_manager)
                except Exception as exc:  # noqa: BLE001 - one bad connection mustn't stop following the rest
                    print(f"following {name!r} to {address} failed: {exc!r}")
        await asyncio.sleep(interval)


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, registry: AnnounceRegistry, manager, modbus_manager, on_change=None):
        self.registry = registry
        self.manager = manager
        self.modbus_manager = modbus_manager
        self.on_change = on_change

    def datagram_received(self, data, addr):
        got = self.registry.receive(data, addr[0])
        if got is None:
            return
        rec, moved = got
        if moved:
            print(f"device {rec['name']!r} announced a new address: {moved} -> {rec['address']}")
        # Follow on every announcement, not only on a move: a connection
        # created (or edited) after the device last moved still gets fixed.
        task = asyncio.get_running_loop().create_task(follow(rec["name"], rec["address"], self.manager, self.modbus_manager))
        if self.on_change:
            task.add_done_callback(lambda t: t.exception() is None and t.result() and self.on_change())


async def start_listener(registry: AnnounceRegistry, manager, modbus_manager, host: str, port: int, on_change=None):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: _Protocol(registry, manager, modbus_manager, on_change), local_addr=(host, port))
    print(f"device announce: listening on udp {host}:{port}"
          f" (allow {', '.join(map(str, registry.allow)) or 'any'}; {'signed only' if registry.key else 'unsigned accepted'})")
    return transport


if __name__ == "__main__":
    main()
