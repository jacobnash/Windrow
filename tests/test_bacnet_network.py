"""Baler#126: BBMD / foreign-device config, routers, and routed (net:mac)
device addresses. No device needed - the live versions, against
windrow.mock_router, are in test_bacnet_routed_live.py."""

import asyncio
import types

import pytest
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address

from windrow import bacnet_network, discovery
from windrow import connection_manager as cm
from windrow.device_registry import DeviceRegistry

# --- addresses ---


@pytest.mark.parametrize("value, network, mac", [
    ("10.0.0.5", None, "10.0.0.5"),
    ("10.0.0.5:47809", None, "10.0.0.5:47809"),
    ("5:12", 5, "12"),  # MS/TP MAC 12 on network 5
    ("5:0x0c", 5, "12"),
    ("7:10.0.1.20", 7, "10.0.1.20"),  # a BACnet/IP device on another IP network, behind a router
])
def test_device_addresses_local_and_routed(value, network, mac):
    parsed = bacnet_network.parse_device_address(value)
    assert (parsed["network"], parsed["mac"]) == (network, mac)


@pytest.mark.parametrize("value", ["*:*", "5:*", "", None, "not an address", 7])
def test_broadcasts_and_garbage_are_not_device_addresses(value):
    with pytest.raises(ValueError):
        bacnet_network.parse_device_address(value)


def test_address_key_keeps_routed_addresses_whole():
    assert bacnet_network.address_key("10.0.0.5:47808") == "10.0.0.5"
    assert bacnet_network.address_key("10.0.0.5") == "10.0.0.5"
    assert bacnet_network.address_key("5:12") == "5:12"
    assert bacnet_network.address_key("5:12") != bacnet_network.address_key("5:13")


def test_describe_source_of_a_routed_i_am():
    assert bacnet_network.describe_source(Address("2:5")) == {"address": "2:5", "network": 2, "mac": "5"}
    assert bacnet_network.describe_source(Address("10.0.0.5")) == {"address": "10.0.0.5", "network": None, "mac": "10.0.0.5"}


# --- configuration ---


def _parse(monkeypatch, env: dict, argv=()):
    for key in ("WINDROW_BACNET_FOREIGN_BBMD", "WINDROW_BACNET_FOREIGN_TTL", "WINDROW_BACNET_BBMD_BDT", "WINDROW_BACNET_NETWORK"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    parser = SimpleArgumentParser()
    bacnet_network.add_network_args(parser)
    return parser.parse_args(list(argv))


def test_foreign_device_from_env(monkeypatch):
    args = _parse(monkeypatch, {"WINDROW_BACNET_FOREIGN_BBMD": "10.1.0.1", "WINDROW_BACNET_FOREIGN_TTL": "120"})
    assert (args.foreign, args.ttl, args.bbmd) == ("10.1.0.1", 120, None)


def test_bbmd_from_env_including_no_peers(monkeypatch):
    assert _parse(monkeypatch, {"WINDROW_BACNET_BBMD_BDT": "10.1.0.1/24, 10.2.0.1/24"}).bbmd == ["10.1.0.1/24", "10.2.0.1/24"]
    assert _parse(monkeypatch, {"WINDROW_BACNET_BBMD_BDT": ""}).bbmd == []


def test_command_line_beats_env(monkeypatch):
    args = _parse(monkeypatch, {"WINDROW_BACNET_FOREIGN_BBMD": "10.1.0.1", "WINDROW_BACNET_NETWORK": "9"},
                  ["--foreign", "10.9.0.1", "--network", "3"])
    assert (args.foreign, args.network) == ("10.9.0.1", 3)


def test_nothing_set_leaves_bacpypes_defaults(monkeypatch):
    args = _parse(monkeypatch, {})
    assert args.foreign is None and args.bbmd is None


# --- routes ---


class FakeCache:
    def __init__(self):
        self.path_info = {(None, 2): (Address("10.0.0.1"), "available"), (None, 3): (Address("10.0.0.1"), "available")}
        self.router_dnets = {(None, Address("10.0.0.1")): {2, 3}}


def test_forget_route_drops_only_that_network():
    app = types.SimpleNamespace(nsap=types.SimpleNamespace(router_info_cache=FakeCache()))
    assert bacnet_network.forget_route(app, "2:5") is True
    assert list(app.nsap.router_info_cache.path_info) == [(None, 3)]
    assert app.nsap.router_info_cache.router_dnets[(None, Address("10.0.0.1"))] == {3}
    assert bacnet_network.forget_route(app, "2:5") is False  # nothing left to forget
    assert bacnet_network.forget_route(app, "10.0.0.7") is False  # local: no route
    assert bacnet_network.forget_route(None, "2:5") is False


def test_poller_forgets_the_route_after_repeated_failures(tmp_path, monkeypatch):
    forgotten = []

    async def dead(app, address, objid):
        raise TimeoutError()

    monkeypatch.setattr(cm, "read_present_value", dead)
    monkeypatch.setattr(cm, "MAX_FAILURE_BACKOFF", 0.02)
    monkeypatch.setattr(cm.bacnet_network, "forget_route", lambda app, address: forgotten.append(address) or True)

    async def run():
        mgr = cm.ConnectionManager(app=None, mqtt_client=None, state_file=str(tmp_path / "c.json"))
        task = asyncio.create_task(mgr._poll_point({"device_address": "2:5", "poll_interval": 0.001, "topic_prefix": "fbf/t"},
                                                   {"object_identifier": "analog-value,1", "label": "p"}))
        await asyncio.sleep(0.3)
        task.cancel()

    asyncio.run(run())
    assert forgotten and set(forgotten) == {"2:5"}


def test_network_status_of_a_foreign_device():
    from bacpypes3.ipv4.service import BIPForeign

    link = BIPForeign.__new__(BIPForeign)
    link.bbmdRegistrationStatus = 0
    link.bbmdAddress = Address("10.1.0.1")
    link.bbmdTimeToLive = 60
    cache = FakeCache()
    app = types.SimpleNamespace(link_layers={"np": link},
                                nsap=types.SimpleNamespace(adapters={1: object()}, router_info_cache=cache))
    status = bacnet_network.network_status(app)
    assert status["mode"] == "foreign" and status["registration"] == "registered" and status["ttl"] == 60
    assert status["routes"] == [{"network": 2, "router": "10.0.0.1"}, {"network": 3, "router": "10.0.0.1"}]
    link.bbmdRegistrationStatus = 0x30  # Register-Foreign-Device NAK
    assert bacnet_network.network_status(app)["registration"] == "rejected (48)"


def test_network_status_without_an_app():
    assert bacnet_network.network_status(None)["mode"] == "normal"


def test_discover_routers_collects_networks_per_router():
    answers = [
        (None, types.SimpleNamespace(pduSource=Address("10.0.0.1"), iartnNetworkList=[2, 3])),
        (None, types.SimpleNamespace(pduSource=Address("10.0.0.9"), iartnNetworkList=[7])),
    ]

    class Nse:
        async def who_is_router_to_network(self, network=None):
            return answers

    routers = asyncio.run(bacnet_network.discover_routers(types.SimpleNamespace(nse=Nse())))
    assert routers == [{"address": "10.0.0.1", "networks": [2, 3]}, {"address": "10.0.0.9", "networks": [7]}]


# --- discovery ---


class WhoIsApp:
    def __init__(self, i_ams=()):
        self.sent_to = []
        self.i_ams = list(i_ams)

    async def who_is(self, address=None, **kw):
        self.sent_to.append(str(address) if address is not None else None)
        return self.i_ams


def _i_am(instance, source):
    return types.SimpleNamespace(iAmDeviceIdentifier=("device", instance), pduSource=Address(source), vendorID=15,
                                 maxAPDULengthAccepted=480)


def test_discovery_broadcasts_globally_by_default_and_reports_routed_devices():
    app = WhoIsApp([_i_am(4242, "2:5"), _i_am(4242, "2:5"), _i_am(3456, "10.0.0.5")])
    devices = asyncio.run(discovery.discover_devices(app))
    assert app.sent_to == ["*:*"]
    assert devices == [  # the duplicate (two paths to one device) folded
        {"device_instance": 4242, "address": "2:5", "network": 2, "mac": "5", "vendor_id": 15, "max_apdu": 480},
        {"device_instance": 3456, "address": "10.0.0.5", "network": None, "mac": "10.0.0.5", "vendor_id": 15, "max_apdu": 480},
    ]


def test_discovery_one_network_or_local_only():
    app = WhoIsApp()
    asyncio.run(discovery.discover_devices(app, network=5))
    asyncio.run(discovery.discover_devices(app, global_broadcast=False))
    asyncio.run(discovery.discover_devices(app, address="5:12"))
    assert app.sent_to == ["5:*", None, "5:12"]
    with pytest.raises(ValueError):
        asyncio.run(discovery.discover_devices(app, network=0))


# --- connections and the device registry with routed addresses ---


def test_create_connection_takes_a_routed_address_and_refuses_a_broadcast(tmp_path, monkeypatch):
    async def read(app, address, objid):
        return 1.0

    monkeypatch.setattr(cm, "read_present_value", read)

    class Mqtt:
        def is_connected(self):
            return True

        def publish(self, *a, **k):
            pass

    async def run():
        mgr = cm.ConnectionManager(app=None, mqtt_client=Mqtt(), state_file=str(tmp_path / "c.json"))
        record = await mgr.create_connection("2:5", 4242, "fbf/vav-9", [{"object_identifier": "analog-value,1", "label": "zt"}])
        assert record["device_address"] == "2:5"
        with pytest.raises(ValueError, match="broadcast"):
            await mgr.create_connection("2:*", 4242, "fbf/vav-9", [{"object_identifier": "analog-value,1", "label": "zt"}])
        await mgr.delete_connection(record["id"])
        await asyncio.sleep(0.01)  # let the cancelled poll task finish

    asyncio.run(run())


def test_registry_keeps_two_devices_on_one_network_apart(tmp_path):
    """Splitting "2:5" on ':' like an ip:port made every device on network 2
    the same host "2"."""
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    connections = [{"id": "conn-5", "device_instance": 4242, "device_address": "2:5"}]
    devices = [{"device_instance": 4242, "address": "2:5", "vendor_id": 1},
               {"device_instance": 4243, "address": "2:6", "vendor_id": 1}]
    asyncio.run(registry.reconcile_bacnet(devices, connections))
    asyncio.run(registry.reconcile_bacnet(devices, connections))  # a rescan updates, doesn't duplicate
    by_address = {d["address"]: d for d in registry.list()}
    assert set(by_address) == {"2:5", "2:6"}
    assert by_address["2:5"]["status"] == "provisioned" and by_address["2:6"]["status"] == "pending"

    added = asyncio.run(registry.add_device({"protocol": "bacnet", "device_address": "2:6", "device_instance": 4243}))
    assert added is by_address["2:6"]
    with pytest.raises(ValueError):
        asyncio.run(registry.add_device({"protocol": "bacnet", "device_address": "2:*", "device_instance": 1}))


def test_registry_directed_who_is_for_a_routed_connection_the_broadcast_missed(tmp_path, monkeypatch):
    calls = []

    async def fake_discover(app, address=None, timeout=None, **kw):
        calls.append(address)
        if address is None:
            return [{"device_instance": 4242, "address": "2:5", "vendor_id": 1}]
        return []

    monkeypatch.setattr(discovery, "discover_devices", fake_discover)
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    asyncio.run(registry.scan_bacnet(object(), [
        {"id": "a", "device_instance": 4242, "device_address": "2:5"},  # heard: no probe
        {"id": "b", "device_instance": 4243, "device_address": "2:6"},  # same network, not heard: probe it
    ]))
    assert calls == [None, "2:6"]
