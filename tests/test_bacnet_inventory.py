"""Windrow#24: each device's vendor, model, firmware, application software,
serial number and location, read at connection start and on every
rediscovery, published on its connections' equip/tags, and a change
(controller swapped, or re-flashed) noticed. Fakes stand in for devices,
plus one read of the real mock device over loopback."""

import asyncio
import contextlib
import json
import random
import socket

import pytest
from bacpypes3.apdu import AbortPDU, ConfirmedServiceChoice, Error, RejectPDU
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import ErrorType

from windrow import bacnet_inventory, device_registry
from windrow import connection_manager as cm

UNKNOWN = ErrorType(errorClass="property", errorCode="unknownProperty")

CONTROLLER = {"object-name": "AHU-1 Controller", "vendor-name": "Acme Controls", "vendor-identifier": 1234,
              "model-name": "AC-500", "firmware-revision": "4.2.1", "application-software-version": "AHU v7",
              "serial-number": "SN-0001", "location": "Mech Room 2"}


class FakeDevice:
    """read_property_multiple / read_property against one device object.
    rpm: "ok" | "reject" (raised) | "returned-reject" | "timeout"."""

    def __init__(self, props: dict, rpm="ok", single_timeout=False):
        self.props = props
        self.rpm = rpm
        self.single_timeout = single_timeout
        self.rpm_calls = 0
        self.single_calls = []

    async def read_property_multiple(self, address, parameter_list):
        self.rpm_calls += 1
        if self.rpm == "reject":
            raise RejectPDU(reason="unrecognizedService")
        if self.rpm == "abort":
            raise AbortPDU(reason="segmentationNotSupported")
        if self.rpm == "returned-reject":
            return RejectPDU(reason="unrecognizedService")
        if self.rpm == "timeout":
            await asyncio.sleep(10)
        objid, props = parameter_list
        return [(objid, p, None, self.props.get(p, UNKNOWN)) for p in props]

    async def read_property(self, address, objid, prop):
        self.single_calls.append(prop)
        if self.single_timeout:
            await asyncio.sleep(10)
        if prop not in self.props:
            raise Error(service_choice=ConfirmedServiceChoice.readProperty, errorClass="property", errorCode="unknownProperty")
        return self.props[prop]


def _read(device, timeout=0.05):
    return asyncio.run(bacnet_inventory.read_inventory(device, "10.0.0.5", 7, timeout=timeout))


FULL = {"instance": 7, "name": "AHU-1 Controller", "vendor_name": "Acme Controls", "vendor_id": 1234,
        "model_name": "AC-500", "firmware_revision": "4.2.1", "application_software_version": "AHU v7",
        "serial_number": "SN-0001", "location": "Mech Room 2"}


def test_one_rpm_reads_the_whole_inventory():
    device = FakeDevice(CONTROLLER)
    assert _read(device) == FULL
    assert device.rpm_calls == 1 and device.single_calls == []


def test_what_the_device_doesnt_have_is_left_out():
    # no serial-number (before protocol revision 22), an empty location
    device = FakeDevice({k: v for k, v in CONTROLLER.items() if k != "serial-number"} | {"location": "  "})
    inventory = _read(device)
    assert "serial_number" not in inventory and "location" not in inventory
    assert inventory["model_name"] == "AC-500"


@pytest.mark.parametrize("rpm", ["reject", "abort", "returned-reject"])
def test_a_device_without_rpm_is_read_property_by_property(rpm):
    device = FakeDevice({k: v for k, v in CONTROLLER.items() if k != "serial-number"}, rpm=rpm)
    inventory = _read(device)
    assert inventory == {k: v for k, v in FULL.items() if k != "serial_number"}
    assert len(device.single_calls) == len(bacnet_inventory.INVENTORY_PROPERTIES)


def test_a_device_that_answers_nothing_is_an_error_not_an_empty_inventory():
    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        _read(FakeDevice(CONTROLLER, rpm="reject", single_timeout=True))


# --- what counts as a change ---


def test_a_new_serial_or_model_is_a_replaced_controller():
    change = bacnet_inventory.compare(FULL, FULL | {"serial_number": "SN-0002", "firmware_revision": "5.0"})
    assert change == {"kind": "replaced", "changed": {"serial_number": ["SN-0001", "SN-0002"],
                                                      "firmware_revision": ["4.2.1", "5.0"]}}


def test_new_firmware_alone_is_a_software_change():
    assert bacnet_inventory.compare(FULL, FULL | {"firmware_revision": "4.3.0"})["kind"] == "software"


def test_nothing_to_compare_or_nothing_changed_is_no_change():
    assert bacnet_inventory.compare(None, FULL) is None
    assert bacnet_inventory.compare(FULL, dict(FULL)) is None
    # a property the device stopped answering isn't a swap; a renamed device isn't either
    assert bacnet_inventory.compare(FULL, {k: v for k, v in FULL.items() if k != "serial_number"}) is None
    assert bacnet_inventory.compare(FULL, FULL | {"name": "AHU-1 (renamed)", "location": "Roof"}) is None


# --- published on the device's connections' equip/tags ---


class Client:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))

    def equip_tags(self):
        return {t: json.loads(json.loads(p)["value"])["tags"] for t, p in self.published if t.endswith("/equip/tags")}


def _manager(tmp_path, app=None):
    manager = cm.ConnectionManager(app, Client(), str(tmp_path / "c.json"))
    for cid, prefix, address, instance in [("a", "fbf/ahu-1", "10.0.0.5:47808", 7), ("b", "fbf/ahu-1-fans", "10.0.0.5", 7),
                                           ("c", "fbf/vav-9", "10.0.0.6", 9)]:
        manager.connections[cid] = {"id": cid, "topic_prefix": prefix, "device_address": address,
                                    "device_instance": instance, "points": [{"label": "p", "object_identifier": "analog-value,1"}]}
    return manager


def test_an_inventory_is_published_on_every_connection_to_that_device(tmp_path):
    manager = _manager(tmp_path, app=FakeDevice(CONTROLLER))
    asyncio.run(manager.refresh_inventory("10.0.0.5", 7))
    tags = manager.mqtt_client.equip_tags()
    assert set(tags) == {"fbf/ahu-1/equip/tags", "fbf/ahu-1-fans/equip/tags"}  # not the VAV on another device
    assert tags["fbf/ahu-1/equip/tags"] == {
        "bacnetDeviceInstance": 7, "bacnetDeviceName": "AHU-1 Controller", "bacnetVendorName": "Acme Controls",
        "bacnetVendorId": 1234, "bacnetModelName": "AC-500", "bacnetFirmwareRevision": "4.2.1",
        "bacnetApplicationSoftwareVersion": "AHU v7", "bacnetSerialNumber": "SN-0001", "bacnetLocation": "Mech Room 2"}


def test_an_unchanged_inventory_isnt_republished_and_a_failed_read_keeps_the_last(tmp_path, monkeypatch):
    monkeypatch.setattr(bacnet_inventory, "INVENTORY_TIMEOUT", 0.05)
    device = FakeDevice(CONTROLLER)
    manager = _manager(tmp_path, app=device)

    async def run():
        await manager.refresh_inventory("10.0.0.5", 7)
        count = len(manager.mqtt_client.published)
        await manager.refresh_inventory("10.0.0.5", 7)
        assert len(manager.mqtt_client.published) == count
        device.rpm, device.single_timeout = "reject", True
        assert await manager.refresh_inventory("10.0.0.5", 7) is None
        assert len(manager.mqtt_client.published) == count

    asyncio.run(run())
    assert manager.inventory[(7, "10.0.0.5")]["serial_number"] == "SN-0001"


def test_a_swapped_controller_republishes_its_equip_tags(tmp_path):
    device = FakeDevice(CONTROLLER)
    manager = _manager(tmp_path, app=device)

    async def run():
        await manager.refresh_inventory("10.0.0.5", 7)
        device.props = CONTROLLER | {"serial-number": "SN-0999", "model-name": "AC-600"}
        await manager.refresh_inventory("10.0.0.5", 7)

    asyncio.run(run())
    last = [json.loads(json.loads(p)["value"])["tags"] for t, p in manager.mqtt_client.published if t == "fbf/ahu-1/equip/tags"][-1]
    assert last["bacnetSerialNumber"] == "SN-0999" and last["bacnetModelName"] == "AC-600"


def test_equip_tags_without_an_inventory_are_as_before(tmp_path):
    manager = _manager(tmp_path)
    manager._publish_equip_membership("fbf/ahu-1", [{"label": "p"}], manager.connections["a"])
    assert manager.mqtt_client.equip_tags() == {"fbf/ahu-1/equip/tags": {}}


# --- rediscovery: the registry keeps the history ---


def test_rediscovery_records_the_inventory_and_its_changes(tmp_path):
    registry = device_registry.DeviceRegistry(str(tmp_path / "devices.json"))
    manager = _manager(tmp_path)
    device = FakeDevice(CONTROLLER)
    found = [{"device_instance": 7, "address": "10.0.0.5", "network": None, "mac": None, "vendor_id": 1234, "max_apdu": 1476}]

    async def run():
        await registry.reconcile_bacnet(found, list(manager.connections.values()))
        await registry.refresh_inventories(device, found, manager)
        device.props = CONTROLLER | {"serial-number": "SN-0999"}
        await registry.refresh_inventories(device, found, manager)

    asyncio.run(run())
    (record,) = registry.list()
    assert record["inventory"]["serial_number"] == "SN-0999"
    assert [h["kind"] for h in record.get("inventory_history", [])] == ["replaced"]
    assert record["inventory_history"][0]["changed"] == {"serial_number": ["SN-0001", "SN-0999"]}
    # handed to the connection manager too, without a second read
    assert manager.inventory[(7, "10.0.0.5")]["serial_number"] == "SN-0999"
    assert device.rpm_calls == 2
    # persisted
    reloaded = device_registry.DeviceRegistry(str(tmp_path / "devices.json"))
    asyncio.run(reloaded.start())
    assert reloaded.list()[0]["inventory"]["serial_number"] == "SN-0999"


# --- the real mock device, over loopback ---


def _free_udp_port() -> int:
    for _ in range(20):
        port = random.randint(40000, 60000)
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    pytest.skip("no free UDP port on loopback")


def test_inventory_of_the_mock_device_over_loopback():
    from bacpypes3.app import Application

    from windrow.mock_device import MockDevice

    device_port, client_port = _free_udp_port(), _free_udp_port()

    async def run():
        mock = MockDevice(SimpleArgumentParser().parse_args(
            ["--address", f"127.0.0.1/32:{device_port}", "--instance", "3456", "--name", "mock-ahu"]))
        client = Application.from_args(SimpleArgumentParser().parse_args(
            ["--address", f"127.0.0.1/32:{client_port}", "--instance", "4194001"]))
        try:
            await asyncio.sleep(0.5)
            return await bacnet_inventory.read_inventory(client, f"127.0.0.1:{device_port}", 3456)
        finally:
            client.close()
            mock.app.close()

    inventory = asyncio.run(run())
    assert inventory["instance"] == 3456 and inventory["name"] == "mock-ahu"
    assert inventory["vendor_name"] and inventory["model_name"] and inventory["firmware_revision"]
    assert "application_software_version" in inventory
