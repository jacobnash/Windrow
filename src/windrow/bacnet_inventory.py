"""
Device inventory for Baler's asset registry (Windrow#24).

A BACnet device object says what it is: vendor, model, firmware,
application software version, often a serial number, and where it is.
Windrow reads that when a connection starts and again on every periodic
rediscovery, and publishes it on each of the device's connections'
`{prefix}/equip/tags` (inventory_tags), so Baler can tell when the
controller behind a piece of equipment was swapped or re-flashed.

Read-only: one ReadPropertyMultiple of the device object, or single
ReadProperty requests for a device that can't do RPM. Every property is
optional - `serial-number` only exists from protocol revision 22, and many
controllers leave `location` empty - so the inventory holds what the device
answered and nothing else.
"""

from __future__ import annotations

import asyncio
import os

from windrow.bacnet_values import normalize_value

INVENTORY_TIMEOUT = float(os.environ.get("WINDROW_INVENTORY_TIMEOUT", "10"))

# property -> inventory key
INVENTORY_PROPERTIES = {
    "object-name": "name",
    "vendor-name": "vendor_name",
    "vendor-identifier": "vendor_id",
    "model-name": "model_name",
    "firmware-revision": "firmware_revision",
    "application-software-version": "application_software_version",
    "serial-number": "serial_number",
    "location": "location",
}

# A change in any of these means a different controller; firmware and
# application software changing alone is the same controller re-flashed.
HARDWARE_KEYS = ("vendor_id", "vendor_name", "model_name", "serial_number")
SOFTWARE_KEYS = ("firmware_revision", "application_software_version")

# inventory key -> the equip tag it's published as. Raw, like Windrow#19's
# point tags: no Brick/Haystack meaning claimed, hence the prefix.
_TAGS = {
    "instance": "bacnetDeviceInstance",
    "name": "bacnetDeviceName",
    "vendor_name": "bacnetVendorName",
    "vendor_id": "bacnetVendorId",
    "model_name": "bacnetModelName",
    "firmware_revision": "bacnetFirmwareRevision",
    "application_software_version": "bacnetApplicationSoftwareVersion",
    "serial_number": "bacnetSerialNumber",
    "location": "bacnetLocation",
}
INVENTORY_TAGS = tuple(_TAGS.values())


def _clean(value):
    from bacpypes3.basetypes import ErrorType

    if value is None or isinstance(value, ErrorType):
        return None
    value = normalize_value(value)
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


async def read_inventory(app, address: str, device_instance: int, timeout: float | None = None) -> dict:
    """{"instance", and whichever of INVENTORY_PROPERTIES' keys the device
    answered}. Raises TimeoutError/RuntimeError only when the device answers
    nothing at all; a missing property is just a missing key."""
    from bacpypes3.apdu import ErrorRejectAbortNack
    from bacpypes3.primitivedata import ObjectIdentifier

    timeout = INVENTORY_TIMEOUT if timeout is None else timeout
    device_id = ObjectIdentifier(f"device,{device_instance}")
    props = list(INVENTORY_PROPERTIES)
    inventory: dict = {"instance": device_instance}
    try:
        results = await asyncio.wait_for(app.read_property_multiple(address, [device_id, props]), timeout)
    except ErrorRejectAbortNack as exc:  # BaseException in bacpypes3: no RPM, or an answer too big
        results = exc
    if isinstance(results, list):
        for _objid, prop_id, _index, value in results:
            key = INVENTORY_PROPERTIES.get(str(prop_id))
            value = _clean(value)
            if key and value is not None:
                inventory[key] = value
        return inventory

    # No RPM: one ReadProperty per property, each bounded
    answered = False
    for prop in props:
        try:
            value = await asyncio.wait_for(app.read_property(address, device_id, prop), timeout)
        except (TimeoutError, asyncio.TimeoutError):
            if not answered:
                raise
            continue
        except ErrorRejectAbortNack:  # unknown-property and friends: the device answered
            answered = True
            continue
        answered = True
        value = _clean(value)
        if value is not None:
            inventory[INVENTORY_PROPERTIES[prop]] = value
    if not answered:
        raise RuntimeError(f"{address} answered no inventory property of device,{device_instance}")
    return inventory


def compare(old: dict | None, new: dict) -> dict | None:
    """What changed between two inventories of one device: None if nothing
    (or no previous), else {"kind": "replaced" | "software", "changed":
    {key: [old, new]}}. Only keys both reads have count - a device that
    stops answering serial-number one day hasn't been swapped."""
    if not old:
        return None
    changed = {
        key: [old[key], new[key]]
        for key in (*HARDWARE_KEYS, *SOFTWARE_KEYS)
        if key in old and key in new and old[key] != new[key]
    }
    if not changed:
        return None
    kind = "replaced" if any(key in changed for key in HARDWARE_KEYS) else "software"
    return {"kind": kind, "changed": changed}


def inventory_tags(inventory: dict | None) -> dict:
    """The inventory as equip tags (bacnetVendorName, bacnetSerialNumber, ...)."""
    if not inventory:
        return {}
    return {tag: inventory[key] for key, tag in _TAGS.items() if inventory.get(key) is not None}
