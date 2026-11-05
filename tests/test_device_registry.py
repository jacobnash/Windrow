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
