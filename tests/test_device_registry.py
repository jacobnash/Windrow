"""
Pure reconciliation-logic tests - fabricated discovery/connection dicts, no
live device or broker needed. The periodic-scan wiring itself (BACnet
Who-Is / Modbus sweep -> reconcile) is exercised live in test_api.py and
test_modbus_discovery.py instead.
"""

import asyncio

import pytest

from windrow import default_credential_check
from windrow.device_registry import DeviceRegistry


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_real_default_credential_check(monkeypatch):
    """A new-pending Modbus device schedules a real default_credential_check
    against its host, fire-and-forget - fine for a real IP, but these tests
    use fabricated hosts, so a real check would either hang on an
    unroutable address or take several real seconds per test for no
    reason. Stubbed to return instantly; the check itself already has its
    own dedicated tests in test_default_credential_check.py."""
    monkeypatch.setattr(default_credential_check, "check", lambda host, endpoints=None: None)


def test_new_bacnet_device_is_pending(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))

    devices = registry.list()
    assert len(devices) == 1
    assert devices[0]["status"] == "pending"
    assert devices[0]["protocol"] == "bacnet"
    assert devices[0]["default_credential_flag"] is None  # BACnet never gets a check


def test_bacnet_device_matching_a_connection_is_provisioned(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    connections = [{"id": "conn-1", "device_instance": 3456, "device_address": "1.2.3.4:47808"}]
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], connections))

    devices = registry.list()
    assert devices[0]["status"] == "provisioned"
    assert devices[0]["connection_id"] == "conn-1"


def test_ignored_device_stays_ignored_across_rescan(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))
    device_id = registry.list()[0]["id"]

    _run(registry.set_status(device_id, "ignored"))
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))

    assert registry.get(device_id)["status"] == "ignored"


def test_provisioned_reverts_to_pending_when_connection_removed(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    connections = [{"id": "conn-1", "device_instance": 3456, "device_address": "1.2.3.4:47808"}]
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], connections))
    device_id = registry.list()[0]["id"]
    assert registry.get(device_id)["status"] == "provisioned"

    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))

    assert registry.get(device_id)["status"] == "pending"
    assert registry.get(device_id)["connection_id"] is None


def test_new_modbus_device_is_pending(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(
        registry.reconcile_modbus(
            [{"host": "10.0.0.5", "port": 502, "vendor": "Acme", "product_code": "X1", "revision": "2.0"}], []
        )
    )

    devices = registry.list()
    assert len(devices) == 1
    assert devices[0]["status"] == "pending"
    assert devices[0]["protocol"] == "modbus"
    assert devices[0]["vendor"] == "Acme"


def test_modbus_device_matching_a_connection_is_provisioned(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    connections = [{"id": "conn-1", "host": "10.0.0.5", "port": 502}]
    _run(registry.reconcile_modbus([{"host": "10.0.0.5", "port": 502, "vendor": None}], connections))

    devices = registry.list()
    assert devices[0]["status"] == "provisioned"
    assert devices[0]["connection_id"] == "conn-1"


def test_set_credential_persists(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))
    device_id = registry.list()[0]["id"]

    _run(registry.set_credential(device_id, {"username": "admin", "password_ciphertext": "abc"}))

    registry2 = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(registry2.start())
    assert registry2.get(device_id)["credential"] == {"username": "admin", "password_ciphertext": "abc"}


def test_scan_adds_directed_who_is_for_known_addresses_the_broadcast_missed(tmp_path, monkeypatch):
    """Baler#52: the broadcast reaches only devices on Windrow's own port,
    so known connection addresses get a unicast Who-Is too."""
    from windrow import discovery

    calls = []

    async def fake_discover(app, low_limit=None, high_limit=None, address=None, timeout=None):
        calls.append(address)
        if address is None:
            return [{"device_instance": 1, "address": "10.0.0.5", "vendor_id": 15}]
        if address.startswith("10.0.0.9"):
            raise TimeoutError("no answer")
        if address.startswith("172.30.0.10"):
            return [{"device_instance": 3456, "address": "172.30.0.10", "vendor_id": 15}]
        return []

    monkeypatch.setattr(discovery, "discover_devices", fake_discover)
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    connections = [
        {"id": "a", "device_instance": 3456, "device_address": "172.30.0.10:47808"},
        {"id": "b", "device_instance": 3456, "device_address": "172.30.0.10:47808"},  # same device, one probe
        {"id": "c", "device_instance": 1, "device_address": "10.0.0.5:47808"},  # broadcast already heard it
        {"id": "d", "device_instance": 7, "device_address": "10.0.0.9"},  # dead: doesn't stop the pass
    ]
    devices = asyncio.run(registry.scan_bacnet(object(), connections))
    assert calls == [None, "10.0.0.9", "172.30.0.10:47808"]
    assert sorted(d["device_instance"] for d in devices) == [1, 3456]


def test_scan_still_probes_known_addresses_when_the_broadcast_fails(tmp_path, monkeypatch):
    from windrow import discovery

    async def fake_discover(app, address=None, timeout=None, **kw):
        if address is None:
            raise OSError("network unreachable")
        return [{"device_instance": 3456, "address": "172.30.0.10", "vendor_id": 15}]

    monkeypatch.setattr(discovery, "discover_devices", fake_discover)
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    devices = asyncio.run(registry.scan_bacnet(object(), [
        {"id": "a", "device_instance": 3456, "device_address": "172.30.0.10:47808"}]))
    assert [d["device_instance"] for d in devices] == [3456]


# --- add_device (POST /devices, the add_device command) ---


def test_add_bacnet_device_matches_the_record_a_scan_creates(tmp_path):
    added = DeviceRegistry(str(tmp_path / "a.json"))
    record = _run(added.add_device({"protocol": "bacnet", "device_address": "1.2.3.4:47808", "device_instance": 3456}))
    scanned = DeviceRegistry(str(tmp_path / "b.json"))
    _run(scanned.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))

    assert set(record) == set(scanned.list()[0])
    assert record["status"] == "pending"
    assert record["address"] == "1.2.3.4"  # standard port dropped, like discovery reports it
    assert record["vendor_id"] is None


def test_add_bacnet_device_is_idempotent_and_a_later_scan_updates_it(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    first = _run(registry.add_device({"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": 3456}))
    again = _run(registry.add_device({"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": 3456}))
    assert again is first
    _run(registry.reconcile_bacnet([{"device_instance": 3456, "address": "1.2.3.4", "vendor_id": 5}], []))
    assert len(registry.list()) == 1
    assert registry.list()[0]["vendor_id"] == 5

    reloaded = DeviceRegistry(str(tmp_path / "devices.json"))
    _run(reloaded.start())
    assert reloaded.get(first["id"])["vendor_id"] == 5


def test_add_bacnet_device_keeps_a_non_standard_port(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    record = _run(registry.add_device({"protocol": "bacnet", "device_address": "1.2.3.4:47809", "device_instance": 7}))
    assert record["address"] == "1.2.3.4:47809"


def test_add_device_already_polled_comes_back_provisioned(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    record = _run(registry.add_device(
        {"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": 3456},
        [{"id": "conn-1", "device_instance": 3456, "device_address": "1.2.3.4:47808"}],
    ))
    assert record["status"] == "provisioned" and record["connection_id"] == "conn-1"


def test_add_modbus_device(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))

    async def run():
        record = await registry.add_device({"protocol": "modbus", "host": "10.0.0.6", "port": 502, "device_id": 3})
        again = await registry.add_device({"protocol": "modbus", "host": "10.0.0.6"})
        await asyncio.sleep(0.05)  # let the stubbed default-credential check run
        return record, again

    record, again = _run(run())
    assert again is record
    assert record["status"] == "pending" and record["device_id"] == 3
    assert record["default_credential_checked_at"] is not None
    scanned = DeviceRegistry(str(tmp_path / "b.json"))
    _run(scanned.reconcile_modbus([{"host": "10.0.0.7", "port": 502}], []))
    assert set(scanned.list()[0]) <= set(record)


@pytest.mark.parametrize("body", [
    {},
    {"protocol": "knx"},
    {"protocol": "bacnet", "device_instance": 1},
    {"protocol": "bacnet", "device_address": "1.2.3.4"},
    {"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": "1"},
    {"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": 4194303},
    {"protocol": "bacnet", "device_address": " ", "device_instance": 1},
    {"protocol": "modbus"},
    {"protocol": "modbus", "host": "10.0.0.6", "port": 0},
    {"protocol": "modbus", "host": "10.0.0.6", "device_id": -1},
    ["protocol", "bacnet"],
])
def test_add_device_rejects_bad_input(tmp_path, body):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    with pytest.raises(ValueError):
        _run(registry.add_device(body))
    assert registry.list() == []


def test_set_status_rejects_an_unknown_status(tmp_path):
    registry = DeviceRegistry(str(tmp_path / "devices.json"))
    record = _run(registry.add_device({"protocol": "bacnet", "device_address": "1.2.3.4", "device_instance": 1}))
    with pytest.raises(ValueError):
        _run(registry.set_status(record["id"], "deleted"))
    assert record["status"] == "pending"
