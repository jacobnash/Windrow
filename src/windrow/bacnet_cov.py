"""
BACnet COV subscriptions for polled points (Baler#127).

Not bacpypes3's Application.change_of_value context manager, for the same
reason bacnet_read bounds its reads (Baler#84): its SubscribeCOV and renewal
requests await app.request() with no timeout, and a renewal that fails
does so in a task whose exception nobody reads - the subscription silently
lapses. Here every SubscribeCOV is bounded, renewal is the caller's loop
(so a failure is seen and counted), and the caller keeps a slow poll going
underneath so a lapsed subscription costs freshness, not data.

Notifications still arrive through bacpypes3's ChangeOfValueServices: it
looks up app._cov_contexts[(source address, subscriber process id)] and
awaits .put(property_value) on whatever it finds, which is all a
Subscription implements.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import random
from collections.abc import Callable
from typing import Any

from windrow.bacnet_read import READ_TIMEOUT, cast_value, enum_name

# Subscriber process ids are per subscriber, 22 bits. Start somewhere random
# so a restart doesn't reuse the last run's ids while the device still holds
# those subscriptions (it would send us notifications for them under a
# different object; the monitored-object check drops those, but why risk it).
_next_process_id = itertools.count(random.randrange(1, 1 << 21))


class CovUnsupported(RuntimeError):
    """The device refused the subscription itself (no COV for this object,
    or no SubscribeCOV at all), as opposed to not answering."""


# Error codes that mean "this object/device won't do COV" - retrying won't help.
_UNSUPPORTED_CODES = {
    "optionalfunctionalitynotsupported", "servicerequestdenied", "notcovproperty",
    "unknownobject", "unsupportedobjecttype", "covsubscriptionfailed",
}


class Subscription:
    def __init__(self, app, address: str, objid: str, on_value: Callable[[Any], None]):
        self.app = app
        self.address = address
        self.objid = objid
        self.on_value = on_value
        self.process_id = next(_next_process_id) % (1 << 22)
        self._address = None
        self._objid = None
        self._vendor_info = None
        self._registered = False

    async def _resolve(self) -> None:
        from bacpypes3.pdu import Address

        if self._address is None:
            self._address = Address(self.address)
            self._vendor_info = await self.app.get_vendor_info(device_address=self._address)
            self._objid = await self.app.parse_object_identifier(self.objid, vendor_info=self._vendor_info)
        # bacpypes3's notification handlers match on this to find us
        self.monitored_object_identifier = self._objid

    async def subscribe(self, lifetime: int, confirmed: bool = False, timeout: float | None = None) -> None:
        """SubscribeCOV, or renew it (the same request again). Raises
        TimeoutError, CovUnsupported, or RuntimeError for any other refusal."""
        from bacpypes3.apdu import (
            Error,
            RejectPDU,
            SimpleAckPDU,
            SubscribeCOVRequest,
        )

        await self._resolve()
        if not self._registered:
            self.app._cov_contexts[(self._address, self.process_id)] = self
            self._registered = True
        request = SubscribeCOVRequest(
            subscriberProcessIdentifier=self.process_id,
            monitoredObjectIdentifier=self._objid,
            issueConfirmedNotifications=confirmed,
            lifetime=int(lifetime),
            destination=self._address,
        )
        response = await asyncio.wait_for(self.app.request(request), READ_TIMEOUT if timeout is None else timeout)
        if isinstance(response, SimpleAckPDU):
            return
        if isinstance(response, Error) and enum_name(response.errorCode) in _UNSUPPORTED_CODES:
            raise CovUnsupported(f"{self.address} {self.objid}: {response}")
        if isinstance(response, RejectPDU) and enum_name(response.apduAbortRejectReason) == "unrecognizedservice":
            raise CovUnsupported(f"{self.address} doesn't support SubscribeCOV")
        raise RuntimeError(f"{self.address} {self.objid}: SubscribeCOV answered {response!r}")

    async def put(self, property_value) -> None:
        """Called by bacpypes3 for each value in a COV notification."""
        if str(property_value.propertyIdentifier) != "present-value":
            return  # status-flags etc.; only present-value is a reading
        try:
            value = cast_value(self._vendor_info, self._objid, property_value.propertyIdentifier, property_value.value)
        except Exception as exc:  # noqa: BLE001 - a bad notification mustn't break bacpypes3's receive path
            print(f"COV notification from {self.address} for {self.objid} didn't decode: {exc!r}")
            return
        if value is not None:
            self.on_value(value)

    def close(self) -> None:
        """Stop taking notifications and, best effort, cancel the
        subscription on the device (a cancelled task can't await, so the
        cancel goes in its own bounded task; if it never arrives the
        subscription just expires at its lifetime)."""
        if not self._registered:
            return
        self.app._cov_contexts.pop((self._address, self.process_id), None)
        self._registered = False
        with contextlib.suppress(RuntimeError):  # no running loop: shutting down
            asyncio.get_running_loop().create_task(self._cancel())

    async def _cancel(self) -> None:
        from bacpypes3.apdu import SubscribeCOVRequest

        request = SubscribeCOVRequest(
            subscriberProcessIdentifier=self.process_id,
            monitoredObjectIdentifier=self._objid,
            destination=self._address,
        )
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.app.request(request), READ_TIMEOUT)
