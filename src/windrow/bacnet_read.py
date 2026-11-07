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

read_present_values is the ReadPropertyMultiple counterpart (Baler#127):
one request for many objects' present-value on one device, with the same
two guarantees - bounded by the same timeout, and every result checked
against the question (an object or property nobody asked for raises
MismatchedResponse rather than landing on some other point). A device that
can't do RPM (Reject unrecognized-service, a services-class Error) or can't
fit the answer in one APDU (Abort buffer-overflow/segmentation-not-supported/
apdu-too-long) raises RPMUnsupported, so the caller can shrink the batch or
fall back to single reads.
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


class RPMUnsupported(RuntimeError):
    """The device refused a ReadPropertyMultiple. `too_big` means it
    understood the request but the answer didn't fit (a smaller batch may
    work); otherwise it doesn't do RPM at all."""

    def __init__(self, message: str, too_big: bool = False):
        super().__init__(message)
        self.too_big = too_big


# Abort reasons that mean "the answer was too big for one APDU", not "I don't
# do RPM": the same request split smaller works.
_TOO_BIG_ABORTS = {"bufferoverflow", "segmentationnotsupported", "apdutoolong"}


def enum_name(value) -> str:
    """bacpypes3 enumerations print hyphenated (segmentation-not-supported)
    but are declared camelCase; compare on a form both reduce to."""
    return str(value).replace("-", "").lower()


def cast_value(vendor_info, objid, prop_id, value_any):
    from bacpypes3.primitivedata import Atomic

    object_class = vendor_info.get_object_class(objid[0])
    property_type = object_class.get_property_type(prop_id) if object_class else None
    if not property_type:
        return None
    # null=True for an atomic type: a commandable point relinquished at
    # every level can answer Null
    return value_any.cast_out(property_type, null=issubclass(property_type, Atomic))


async def read_present_value(app, address, objid, prop: str = "present-value", timeout: float | None = None) -> Any:
    from bacpypes3.apdu import (
        ErrorRejectAbortNack,
        ReadPropertyACK,
        ReadPropertyRequest,
    )
    from bacpypes3.pdu import Address

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
    return cast_value(vendor_info, objid, prop_id, response.propertyValue)


async def read_present_values(
    app, address, objids: list[str], prop: str = "present-value", timeout: float | None = None
) -> dict[str, Any]:
    """One ReadPropertyMultiple for `prop` of every object in `objids`.

    Returns {objid as given: value}. A per-object failure the device reports
    inside the answer (unknown-object, say) comes back as that object's
    bacpypes3 ErrorType, and an object missing from the answer is left out,
    so the caller counts both as failed reads without failing its siblings.
    Raises TimeoutError, RPMUnsupported, MismatchedResponse, or ReadError for
    any other Error/Reject/Abort for the whole request."""
    from bacpypes3.apdu import (
        AbortPDU,
        Error,
        ErrorRejectAbortNack,
        ReadPropertyMultipleACK,
        ReadPropertyMultipleRequest,
        RejectPDU,
    )
    from bacpypes3.basetypes import PropertyReference, ReadAccessSpecification
    from bacpypes3.constructeddata import SequenceOf
    from bacpypes3.pdu import Address

    if isinstance(address, str):
        address = Address(address)
    vendor_info = await app.get_vendor_info(device_address=address)
    prop_ref = await app.parse_property_reference(prop, vendor_info=vendor_info)
    prop_id = prop_ref.propertyIdentifier

    asked: dict = {}  # parsed ObjectIdentifier -> the caller's string
    specs = []
    for objid in objids:
        parsed = await app.parse_object_identifier(objid, vendor_info=vendor_info) if isinstance(objid, str) else objid
        if parsed in asked:
            continue
        asked[parsed] = objid
        specs.append(ReadAccessSpecification(
            objectIdentifier=parsed,
            listOfPropertyReferences=[PropertyReference(propertyIdentifier=prop_id)],
        ))

    request = ReadPropertyMultipleRequest(
        listOfReadAccessSpecs=SequenceOf(ReadAccessSpecification)(specs), destination=address
    )
    response = await asyncio.wait_for(app.request(request), READ_TIMEOUT if timeout is None else timeout)

    if isinstance(response, RejectPDU) and enum_name(response.apduAbortRejectReason) == "unrecognizedservice":
        raise RPMUnsupported(f"{address} rejected ReadPropertyMultiple: {response.apduAbortRejectReason}")
    if isinstance(response, AbortPDU):
        reason = str(response.apduAbortRejectReason)
        if enum_name(reason) in _TOO_BIG_ABORTS:
            raise RPMUnsupported(f"{address} aborted a {len(specs)}-object ReadPropertyMultiple: {reason}", too_big=True)
    if isinstance(response, Error) and enum_name(response.errorClass) == "services":
        raise RPMUnsupported(f"{address} refused ReadPropertyMultiple: {response}")
    if isinstance(response, ErrorRejectAbortNack):
        raise ReadError(f"{address} answered ReadPropertyMultiple with {response!r}")
    if not isinstance(response, ReadPropertyMultipleACK):
        raise ReadError(f"{address} answered ReadPropertyMultiple with {response!r}")

    results: dict[str, Any] = {}
    for access_result in response.listOfReadAccessResults:
        objid = access_result.objectIdentifier
        if objid not in asked:
            raise MismatchedResponse(f"asked {address} for {len(asked)} objects, got an answer for {objid}")
        for element in access_result.listOfResults:
            if element.propertyIdentifier != prop_id:
                raise MismatchedResponse(f"asked {address} for {objid} {prop_id}, got {element.propertyIdentifier}")
            if element.readResult.propertyAccessError is not None:
                results[asked[objid]] = element.readResult.propertyAccessError
            else:
                results[asked[objid]] = cast_value(vendor_info, objid, prop_id, element.readResult.propertyValue)
    return results


class ReadError(RuntimeError):
    """The device answered a whole request with an error, reject or abort."""
