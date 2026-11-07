"""read_present_value rejects a ReadPropertyACK for a different object or
property (Baler#61: a late answer to a timed-out read, matched by a reused
invoke ID, put a meter's kWh on an AHU's discharge temperature)."""

import asyncio

import pytest
from bacpypes3.apdu import ReadPropertyACK
from bacpypes3.basetypes import PropertyIdentifier
from bacpypes3.constructeddata import Any
from bacpypes3.local.device import DeviceObject  # noqa: F401 - registers standard object classes
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Real
from bacpypes3.vendor import get_vendor_info

from windrow.bacnet_read import MismatchedResponse, read_present_value


class FakeApp:
    def __init__(self, answer_for: str):
        self.answer_for = answer_for

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    async def parse_property_reference(self, prop, vendor_info=None):
        class Ref:
            propertyIdentifier = PropertyIdentifier(prop)
            propertyArrayIndex = None

        return Ref()

    async def request(self, req):
        ack = ReadPropertyACK(
            objectIdentifier=ObjectIdentifier(self.answer_for),
            propertyIdentifier=req.propertyIdentifier,
            propertyValue=Any(Real(86116.53125)),
        )
        ack.pduSource = req.pduDestination
        return ack


def _read(app, objid):
    return asyncio.run(read_present_value(app, Address("172.30.0.10"), objid))


def test_matching_answer_is_returned():
    assert _read(FakeApp("analog-input,376"), "analog-input,376") == pytest.approx(86116.53125)


def test_answer_for_another_object_is_rejected():
    with pytest.raises(MismatchedResponse, match="analog-input,12"):
        _read(FakeApp("analog-input,376"), "analog-input,12")
