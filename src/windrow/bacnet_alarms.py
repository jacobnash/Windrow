"""
BACnet alarms from intrinsic reporting (Baler#164).

A controller with intrinsic reporting (an object's high/low limit, a
binary point's alarm value, a fault) sends an EventNotification to every
recipient in the object's NotificationClass. Windrow never asked to be one,
and bacpypes3's Application has no handler for those notifications, so
controller alarms never left the site.

- WindrowApplication answers ConfirmedEventNotification (with the SimpleAck
  the device waits for) and UnconfirmedEventNotification, and hands each one
  to its listeners - connection_manager, which publishes it.
- ensure_recipient adds Windrow to a NotificationClass's recipient-list
  (read it, append a Destination if ours isn't there, write it back), and
  remove_recipient takes it out again. A connection does this only when
  `forward_alarms` is set: it changes the controller's configuration.
- get_event_information reads the device's active alarms
  (GetEventInformation, paged), so an alarm raised while Windrow was down or
  not yet registered isn't missed.

Every request has a timeout (Baler#84), and the Error/Reject/Abort that
bacpypes3 raises - a BaseException - comes back as AlarmError
(bacnet_read.request).
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

from bacpypes3.apdu import SimpleAckPDU
from bacpypes3.app import Application

from windrow.bacnet_read import read_present_value, request

# The processIdentifier Windrow puts in its Destinations, so its entry can be
# told apart from other recipients' on the same address and removed later.
ALARM_PROCESS_ID = int(os.environ.get("WINDROW_ALARM_PROCESS_ID", "8742"))


class AlarmError(RuntimeError):
    """The device refused or didn't answer an alarm-setup request."""


class WindrowApplication(Application):
    """bacpypes3's Application plus EventNotification handling. Listeners
    get the request APDU; one that raises doesn't stop the others or the ack."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_listeners: list[Callable[[Any], None]] = []

    def _dispatch_event(self, apdu) -> None:
        for listener in list(self.event_listeners):
            try:
                listener(apdu)
            except Exception as exc:  # noqa: BLE001 - one listener's bug mustn't drop the alarm for the rest
                print(f"event notification listener failed: {exc!r}")

    async def do_ConfirmedEventNotificationRequest(self, apdu) -> None:
        self._dispatch_event(apdu)
        await self.response(SimpleAckPDU(context=apdu))

    async def do_UnconfirmedEventNotificationRequest(self, apdu) -> None:
        self._dispatch_event(apdu)


def _text(value) -> str | None:
    return None if value is None else str(value)


def _timestamp_text(ts) -> str | None:
    """A BACnet TimeStamp as the device sent it: a date-time, a time of day,
    or a sequence number (devices without a clock)."""
    if ts is None:
        return None
    if ts.dateTime is not None:
        return f"{ts.dateTime.date} {ts.dateTime.time}"
    if ts.time is not None:
        return str(ts.time)
    if ts.sequenceNumber is not None:
        return f"#{int(ts.sequenceNumber)}"
    return None


def notification_to_dict(apdu, received_at: float | None = None) -> dict:
    """The parts of an EventNotification Baler's alarm console needs, as
    plain JSON. `ts` is when Windrow received it; `device_timestamp` is the
    device's own, which may be a bare time of day or a sequence number."""
    return {
        "object": str(apdu.eventObjectIdentifier),
        "device": str(apdu.initiatingDeviceIdentifier) if apdu.initiatingDeviceIdentifier is not None else None,
        "event_type": _text(apdu.eventType),
        "from_state": _text(apdu.fromState),
        "to_state": _text(apdu.toState),
        "notify_type": _text(apdu.notifyType),
        "priority": int(apdu.priority) if apdu.priority is not None else None,
        "ack_required": bool(apdu.ackRequired) if apdu.ackRequired is not None else None,
        "message": _text(apdu.messageText),
        "notification_class": int(apdu.notificationClass) if apdu.notificationClass is not None else None,
        "device_timestamp": _timestamp_text(apdu.timeStamp),
        "ts": received_at if received_at is not None else time.time(),
    }


def our_recipient(app, device_address: str):
    """How the controller should address Windrow. A device on this network
    gets our own address (network 0 = its local network): it can send
    straight back without having to find us. A routed device (`net:mac`)
    gets our device identifier - our address means nothing on its network -
    and finds us with Who-Is, which our Application answers."""
    from bacpypes3.basetypes import DeviceAddress, Recipient
    from bacpypes3.local.networkport import NetworkPortObject
    from bacpypes3.pdu import Address

    if Address(device_address).addrType == Address.remoteStationAddr:
        return Recipient(device=app.device_object.objectIdentifier)
    for obj in app.objectIdentifier.values():
        if isinstance(obj, NetworkPortObject) and str(obj.networkType) == "ipv4":
            return Recipient(address=DeviceAddress(networkNumber=0, macAddress=obj.address.addrAddr))
    return Recipient(device=app.device_object.objectIdentifier)


def _same_recipient(destination, recipient, process_id: int) -> bool:
    if int(destination.processIdentifier) != process_id:
        return False
    theirs, ours = destination.recipient, recipient
    if ours.device is not None:
        return theirs.device is not None and theirs.device == ours.device
    return (theirs.address is not None and int(theirs.address.networkNumber) == int(ours.address.networkNumber)
            and bytes(theirs.address.macAddress) == bytes(ours.address.macAddress))


async def _write(app, address: str, objid: str, prop: str, value, timeout: float | None = None) -> None:
    from bacpypes3.apdu import ErrorRejectAbortNack, SimpleAckPDU, WritePropertyRequest
    from bacpypes3.constructeddata import Any as AnyValue
    from bacpypes3.pdu import Address

    vendor_info = await app.get_vendor_info(device_address=Address(address))
    parsed = await app.parse_object_identifier(objid, vendor_info=vendor_info)
    prop_ref = await app.parse_property_reference(prop, vendor_info=vendor_info)
    object_class = vendor_info.get_object_class(parsed[0])
    property_type = object_class.get_property_type(prop_ref.propertyIdentifier)
    answer = await request(app, WritePropertyRequest(
        objectIdentifier=parsed, propertyIdentifier=prop_ref.propertyIdentifier,
        propertyValue=AnyValue(property_type(value)), destination=Address(address),
    ), timeout)
    if isinstance(answer, ErrorRejectAbortNack) or not isinstance(answer, SimpleAckPDU):
        raise AlarmError(f"{address} refused writing {objid} {prop}: {answer}")


async def _read(app, address: str, objid: str, prop: str):
    from bacpypes3.apdu import ErrorRejectAbortNack

    value = await read_present_value(app, address, objid, prop)
    if value is None or isinstance(value, ErrorRejectAbortNack):
        raise AlarmError(f"{address} answered {objid} {prop} with {value!r}")
    return value


async def notification_classes(app, address: str, device_instance: int) -> list[str]:
    """Every notification-class object in the device's object-list."""
    object_list = await _read(app, address, f"device,{device_instance}", "object-list")
    return [f"notification-class,{objid[1]}" for objid in object_list if str(objid[0]) == "notification-class"]


async def ensure_recipient(app, address: str, notification_class: str, process_id: int = ALARM_PROCESS_ID) -> bool:
    """Add Windrow to the class's recipient-list if it isn't there; True if
    it was added. Every day, all day, all three transitions, confirmed (so a
    lost notification is retried by the device)."""
    from bacpypes3.basetypes import Destination

    recipient = our_recipient(app, address)
    current = list(await _read(app, address, notification_class, "recipient-list"))
    if any(_same_recipient(d, recipient, process_id) for d in current):
        return False
    current.append(Destination(
        validDays=[1] * 7, fromTime=(0, 0, 0, 0), toTime=(23, 59, 59, 99), recipient=recipient,
        processIdentifier=process_id, issueConfirmedNotifications=True, transitions=[1, 1, 1],
    ))
    await _write(app, address, notification_class, "recipient-list", current)
    return True


async def remove_recipient(app, address: str, notification_class: str, process_id: int = ALARM_PROCESS_ID) -> bool:
    recipient = our_recipient(app, address)
    current = list(await _read(app, address, notification_class, "recipient-list"))
    kept = [d for d in current if not _same_recipient(d, recipient, process_id)]
    if len(kept) == len(current):
        return False
    await _write(app, address, notification_class, "recipient-list", kept)
    return True


async def get_event_information(app, address: str, timeout: float | None = None, max_pages: int = 50) -> list[dict]:
    """The device's objects not in a normal event state, or with
    transitions not yet acknowledged: [{object, event_state, acked,
    notify_type, timestamps}]."""
    from bacpypes3.apdu import (
        ErrorRejectAbortNack,
        GetEventInformationACK,
        GetEventInformationRequest,
    )
    from bacpypes3.pdu import Address

    events: list[dict] = []
    last = None
    for _ in range(max_pages):
        req = GetEventInformationRequest(destination=Address(address))
        if last is not None:
            req.lastReceivedObjectIdentifier = last
        answer = await request(app, req, timeout)
        if isinstance(answer, ErrorRejectAbortNack) or not isinstance(answer, GetEventInformationACK):
            raise AlarmError(f"{address} answered GetEventInformation with {answer!r}")
        for summary in answer.listOfEventSummaries:
            acked = list(summary.acknowledgedTransitions)
            events.append({
                "object": str(summary.objectIdentifier),
                "event_state": _text(summary.eventState),
                "acked": {"to_offnormal": bool(acked[0]), "to_fault": bool(acked[1]), "to_normal": bool(acked[2])},
                "notify_type": _text(summary.notifyType),
                "timestamps": [_timestamp_text(t) for t in summary.eventTimeStamps],
            })
            last = summary.objectIdentifier
        if not answer.moreEvents:
            break
    return events
