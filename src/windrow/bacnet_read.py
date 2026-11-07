"""
A ReadProperty that checks the answer is for the question.

bacpypes3's Application.read_property returns whatever ReadPropertyACK
comes back for the request's invoke ID, without checking its object and
property against the request. When a read times out, its invoke ID goes back
into the pool. If the device's answer to it arrives late, it can be matched
to the next read that reuses the ID - a different point. Seen live (Baler#61): during a
restart burst, meter-1's 86116.5 kWh total turned up 2 s later as ahu-1's
Discharge_Air_Temp, and a VAV airflow as another VAV's zone temperature.

read_present_value does what read_property does, plus that check: a
response whose objectIdentifier/propertyIdentifier don't match raises
MismatchedResponse, so the poll counts a failed read and publishes nothing
rather than a plausible-looking wrong value.

It also bounds the wait. bacpypes3 0.0.110's Application.request never
returns once the device stops answering - no APDU timeout fires, and the
await stays pending even after the device comes back (reproduced against
windrow.mock_device: kill it mid-poll, restart it, the read never resolves).
That is Baler#84: a device reboot silenced its connection until windrow_api
restarted. A read that takes longer than READ_TIMEOUT raises TimeoutError,
the caller counts a failure and retries, and the next request after the
device is back succeeds.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

# Seconds one ReadProperty may take before it counts as failed. Above
# bacpypes3's own APDU timeout x retries (3 s x 3), so a slow device that
# does answer isn't cut off.
READ_TIMEOUT = float(os.environ.get("WINDROW_READ_TIMEOUT", "10"))


class MismatchedResponse(RuntimeError):
    """The device answered, but for a different object or property."""


async def read_present_value(app, address, objid, prop: str = "present-value", timeout: float | None = None) -> Any:
    from bacpypes3.apdu import ErrorRejectAbortNack, ReadPropertyACK, ReadPropertyRequest
    from bacpypes3.pdu import Address
    from bacpypes3.primitivedata import Atomic

    if isinstance(address, str):
        address = Address(address)
    vendor_info = await app.get_vendor_info(device_address=address)
    if isinstance(objid, str):
        objid = await app.parse_object_identifier(objid, vendor_info=vendor_info)
    prop_ref = await app.parse_property_reference(prop, vendor_info=vendor_info)
    prop_id = prop_ref.propertyIdentifier

    response = await asyncio.wait_for(
        app.request(ReadPropertyRequest(objectIdentifier=objid, propertyIdentifier=prop_id, destination=address)),
        READ_TIMEOUT if timeout is None else timeout,
    )
    if isinstance(response, ErrorRejectAbortNack):
        return response  # same as read_property: the caller's normalize/handling decides
    if not isinstance(response, ReadPropertyACK):
        return None
    if response.objectIdentifier != objid or response.propertyIdentifier != prop_id:
        raise MismatchedResponse(
            f"asked {address} for {objid} {prop_id}, got {response.objectIdentifier} {response.propertyIdentifier}"
        )
    object_class = vendor_info.get_object_class(objid[0])
    property_type = object_class.get_property_type(prop_id) if object_class else None
    if not property_type:
        return None
    return response.propertyValue.cast_out(property_type, null=issubclass(property_type, Atomic))
