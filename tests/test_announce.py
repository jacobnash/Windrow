"""Device announce (windrow.announce): devices say "I am here", Windrow
keeps a registry by name and re-points connections whose device_name
matches when the address changes - by announcement first, DNS otherwise.
No sockets, broker or devices: datagrams are built in-process and the real
managers run with fake reads / a fake Modbus client."""

import asyncio
import ipaddress
import json
from datetime import datetime, timezone

import pytest

from windrow import announce
from windrow import connection_manager as cm
from windrow import modbus_connection_manager as mcm

LAN = [ipaddress.ip_network("192.168.128.0/24")]


class FakeMqttClient:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def datagram(name="bacnet", key=None, ts=None, services=None, **extra):
    p = announce.build_announcement(name, services or [{"kind": "bacnet", "port": 47808, "instance": 10228}],
                                    ["192.168.128.228"], key=key, fqdn=f"{name}.lan", now=ts)
    p.update(extra)
    return json.dumps(p).encode()


# --- signing and vetting ---


def test_sign_and_verify_roundtrip():
    p = announce.sign({"v": 1, "name": "x", "ts": "t"}, "k")
    assert announce.verify(p, "k")
    assert not announce.verify(p, "other")
    assert not announce.verify({**p, "name": "y"}, "k")
    assert not announce.verify({"v": 1, "name": "x"}, "k")


def test_address_comes_from_the_source_not_the_payload():
    a = announce.check_announcement(datagram(ips=["10.9.9.9"]), "192.168.128.50", LAN, None, 0)
    assert a["address"] == "192.168.128.50"
    assert a["name"] == "bacnet"


@pytest.mark.parametrize("raw,src,why", [
    (b"not json", "192.168.128.5", "not JSON"),
    (json.dumps({"v": 2, "name": "x"}).encode(), "192.168.128.5", "v1"),
    (json.dumps({"v": 1}).encode(), "192.168.128.5", "name"),
    (b"{}", "10.0.0.5", "outside"),
    (b"x" * (announce.MAX_DATAGRAM + 1), "192.168.128.5", "too large"),
])
def test_rejections(raw, src, why):
    with pytest.raises(announce.Rejected, match=why):
        announce.check_announcement(raw, src, LAN, None, 0)


def test_key_required_when_configured():
    now = 1_800_000_000.0
    with pytest.raises(announce.Rejected, match="signature"):
        announce.check_announcement(datagram(ts=now), "192.168.128.5", LAN, "secret", now)
    with pytest.raises(announce.Rejected, match="signature"):
        announce.check_announcement(datagram(ts=now, key="wrong"), "192.168.128.5", LAN, "secret", now)
    assert announce.check_announcement(datagram(ts=now, key="secret"), "192.168.128.5", LAN, "secret", now)


def test_signed_replay_is_refused():
    now = 1_800_000_000.0
    old = datagram(ts=now - announce.MAX_SKEW - 5, key="secret")
    with pytest.raises(announce.Rejected, match="replayed"):
        announce.check_announcement(old, "192.168.128.5", LAN, "secret", now)


def test_parse_service():
    assert announce.parse_service("bacnet:47808:10228") == {"kind": "bacnet", "port": 47808, "instance": 10228}
    assert announce.parse_service("modbus:502:1") == {"kind": "modbus", "port": 502, "unit": 1}
    with pytest.raises(ValueError):
        announce.parse_service("snmp:161")


@pytest.mark.parametrize("addr,host,want", [
    ("192.168.128.228:47808", "192.168.128.40", "192.168.128.40:47808"),
    ("192.168.128.228", "192.168.128.40", "192.168.128.40"),
    ("192.168.128.228/24:47808", "192.168.128.40", "192.168.128.40/24:47808"),
])
def test_with_host_keeps_port_and_prefix(addr, host, want):
    assert announce._with_host(addr, host) == want
    assert announce._host_of(addr) == "192.168.128.228"


# --- registry ---


def test_registry_tracks_moves_and_persists(tmp_path):
    clock = Clock()
    reg = announce.AnnounceRegistry(str(tmp_path / "a.json"), interval=30, allow=LAN, clock=clock)
    rec, moved = reg.receive(datagram(), "192.168.128.228")
    assert moved is None and rec["address"] == "192.168.128.228"
    clock.t += 30
    rec, moved = reg.receive(datagram(), "192.168.128.40")
    assert moved == "192.168.128.228" and rec["address"] == "192.168.128.40"
    assert rec["history"][-1]["from"] == "192.168.128.228"

    again = announce.AnnounceRegistry(str(tmp_path / "a.json"), interval=30, clock=clock)
    again.load()
    assert again.devices["bacnet"]["address"] == "192.168.128.40"


def test_registry_goes_offline_after_missed_announcements(tmp_path):
    clock = Clock()
    reg = announce.AnnounceRegistry(str(tmp_path / "a.json"), interval=30, clock=clock)
    reg.receive(datagram(), "192.168.128.228")
    assert reg.address("bacnet") == "192.168.128.228"
    clock.t += 30 * announce.STALE_AFTER + 1
    assert reg.address("bacnet") is None
    assert reg.list()[0]["online"] is False
    assert reg.summary() == {"announced": 1, "announced_online": 0, "announce_rejected": 0}


def test_registry_counts_rejections(tmp_path):
    reg = announce.AnnounceRegistry(str(tmp_path / "a.json"), allow=LAN)
    assert reg.receive(datagram(), "10.1.1.1") is None
    assert reg.rejected == 1 and "outside" in reg.last_rejection


# --- following a device: the real managers ---


class FakeModbusClient:
    def __init__(self):
        self.connected = False

    async def connect(self):
        self.connected = True
        return True

    async def read_holding_registers(self, address, count=1, device_id=1):
        class R:
            registers = [70]

            def isError(self):
                return False
        return R()

    def close(self):
        self.connected = False


@pytest.fixture
def managers(tmp_path, monkeypatch):
    async def fake_read(app, address, objid):
        return 70.0

    monkeypatch.setattr(cm, "read_present_value", fake_read)
    monkeypatch.setattr(mcm.mqtt_sink, "publish_reading", lambda *a, **k: None)

    def make():
        client = FakeMqttClient()
        bac = cm.ConnectionManager(app=None, mqtt_client=client, state_file=str(tmp_path / "b.json"))
        mod = mcm.ModbusConnectionManager(client, str(tmp_path / "m.json"))
        monkeypatch.setattr(mod, "_new_client", lambda record: FakeModbusClient())
        return bac, mod
    return make


async def _cleanup(bac, mod):
    for rec in list(bac.connections.values()):
        await bac.delete_connection(rec["id"])
    await mod.close()


def test_follow_repoints_named_connections_only(managers):
    async def run():
        bac, mod = managers()
        points = [{"object_identifier": "analog-value,1", "label": "zone_temp"}]
        named = await bac.create_connection("192.168.128.228:47808", 10228, "fbf/home/bacnet", points,
                                            poll_interval=60, device_name="BACnet")
        other = await bac.create_connection("192.168.128.99:47808", 1, "fbf/x", points, poll_interval=60)
        mb = await mod.create_connection("192.168.128.228", "fbf/home/modbus", {"zone_temp": 0},
                                         poll_interval=60, device_name="bacnet")
        assert named["device_name"] == "bacnet"  # stored lower-case
        changed = await announce.follow("bacnet", "192.168.128.40", bac, mod)
        again = await announce.follow("bacnet", "192.168.128.40", bac, mod)
        out = (changed, again, dict(bac.connections[named["id"]]), dict(bac.connections[other["id"]]),
               dict(mod.connections[mb["id"]]))
        await _cleanup(bac, mod)
        return out

    changed, again, named, other, mb = asyncio.run(run())
    assert {c["kind"] for c in changed} == {"bacnet", "modbus"}
    assert named["device_address"] == "192.168.128.40:47808"
    assert other["device_address"] == "192.168.128.99:47808"
    assert mb["host"] == "192.168.128.40"
    assert again == []  # idempotent


def test_resolver_prefers_announcement_over_dns(managers, tmp_path):
    clock = Clock()
    reg = announce.AnnounceRegistry(str(tmp_path / "a.json"), interval=30, clock=clock)
    reg.receive(datagram("modbus", services=[{"kind": "modbus", "port": 502}]), "192.168.128.31")
    dns = {"modbus": "192.168.128.77", "ahu-1.site.lan": "192.168.128.21"}

    async def run():
        bac, mod = managers()
        await mod.create_connection("192.168.128.230", "fbf/home/modbus", {"zone_temp": 0}, poll_interval=60,
                                    device_name="modbus")
        await bac.create_connection("192.168.128.20:47808", 1, "fbf/dns", [{"object_identifier": "analog-value,1", "label": "t"}],
                                    poll_interval=60, device_name="ahu-1.site.lan")
        task = asyncio.get_running_loop().create_task(announce.run_resolver(reg, bac, mod, 3600, resolver=dns.get))
        for _ in range(200):
            await asyncio.sleep(0.01)
            if ([r["host"] for r in mod.connections.values()] == ["192.168.128.31"]
                    and [r["device_address"] for r in bac.connections.values()] == ["192.168.128.21:47808"]):
                break
        task.cancel()
        out = ([r["host"] for r in mod.connections.values()], [r["device_address"] for r in bac.connections.values()])
        await _cleanup(bac, mod)
        return out

    hosts, addrs = asyncio.run(run())
    assert hosts == ["192.168.128.31"]  # the announcement wins over DNS's .77
    assert addrs == ["192.168.128.21:47808"]  # nothing announced: DNS


def test_device_name_is_editable_and_clearable(managers):
    async def run():
        bac, mod = managers()
        rec = await mod.create_connection("192.168.128.230", "fbf/home/modbus", {"zone_temp": 0}, poll_interval=60)
        a = await mod.update_connection(rec["id"], {"device_name": "Modbus"})
        a = dict(a)
        b = dict(await mod.update_connection(rec["id"], {"device_name": ""}))
        try:
            await mod.update_connection(rec["id"], {"device_name": "two words"})
            err = None
        except ValueError as exc:
            err = exc
        await _cleanup(bac, mod)
        return a, b, err

    a, b, err = asyncio.run(run())
    assert a["device_name"] == "modbus"
    assert "device_name" not in b
    assert err is not None


def test_build_announcement_shape():
    p = announce.build_announcement("bacnet", [{"kind": "bacnet", "port": 47808}], ["192.168.128.228"], key="k",
                                    fqdn="bacnet.lan", now=datetime(2026, 11, 7, tzinfo=timezone.utc).timestamp())
    assert p["v"] == 1 and p["name"] == "bacnet" and p["ts"].startswith("2026-11-07") and announce.verify(p, "k")
