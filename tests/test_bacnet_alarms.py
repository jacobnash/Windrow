"""Baler#164: BACnet alarms from intrinsic reporting. No device needed - the
live version against windrow.mock_device --intrinsic-reporting is in
test_bacnet_alarms_live.py."""

import asyncio
import json
import types

import pytest
from bacpypes3.apdu import (
    ConfirmedEventNotificationRequest,
    ConfirmedServiceChoice,
    Error,
    GetEventInformationACK,
    SimpleAckPDU,
)
from bacpypes3.basetypes import (
    DeviceAddress,
    GetEventInformationEventSummary,
    Recipient,
    TimeStamp,
)
from bacpypes3.local.device import (
    DeviceObject,  # noqa: F401 - registers standard object classes
)
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.vendor import get_vendor_info

from windrow import bacnet_alarms
from windrow import connection_manager as cm


def _notification(objid="analog-value,1", to_state="high-limit", source="10.0.0.5"):
    apdu = ConfirmedEventNotificationRequest(
        processIdentifier=bacnet_alarms.ALARM_PROCESS_ID, initiatingDeviceIdentifier=ObjectIdentifier("device,5"),
        eventObjectIdentifier=ObjectIdentifier(objid), timeStamp=TimeStamp(sequenceNumber=12), notificationClass=1,
        priority=3, eventType="out-of-range", messageText="zone too hot", notifyType="alarm", ackRequired=True,
        fromState="normal", toState=to_state,
    )
    apdu.pduSource = Address(source)
    return apdu


def test_notification_to_dict():
    event = bacnet_alarms.notification_to_dict(_notification(), received_at=100.0)
    assert event == {
        "object": "analog-value,1", "device": "device,5", "event_type": "out-of-range", "from_state": "normal",
        "to_state": "high-limit", "notify_type": "alarm", "priority": 3, "ack_required": True,
        "message": "zone too hot", "notification_class": 1, "device_timestamp": "#12", "ts": 100.0,
    }


def test_windrow_application_acks_confirmed_notifications_and_survives_a_bad_listener():
    seen, responses = [], []
    app = bacnet_alarms.WindrowApplication.__new__(bacnet_alarms.WindrowApplication)
    app.event_listeners = [lambda apdu: 1 / 0, seen.append]

    async def response(apdu):
        responses.append(apdu)

    app.response = response
    asyncio.run(app.do_ConfirmedEventNotificationRequest(_notification()))
    asyncio.run(app.do_UnconfirmedEventNotificationRequest(_notification()))
    assert len(seen) == 2
    assert len(responses) == 1 and isinstance(responses[0], SimpleAckPDU)


# --- recipient-list ---


class NCApp:
    """A device with one notification class. Reads and writes go through
    bacnet_alarms' read_present_value / request, which the fixture fakes."""

    def __init__(self, recipients=None, refuse_write=False):
        self.recipients = list(recipients or [])
        self.refuse_write = refuse_write
        self.writes = 0

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    async def parse_property_reference(self, prop, vendor_info=None):
        from bacpypes3.basetypes import PropertyIdentifier

        return types.SimpleNamespace(propertyIdentifier=PropertyIdentifier(prop), propertyArrayIndex=None)


@pytest.fixture
def nc(monkeypatch):
    app = NCApp()
    our = Recipient(address=DeviceAddress(networkNumber=0, macAddress=Address("10.0.0.9:47808").addrAddr))
    monkeypatch.setattr(bacnet_alarms, "our_recipient", lambda a, address: our)

    async def read(a, address, objid, prop="present-value", timeout=None):
        if prop == "object-list":
            return [ObjectIdentifier("device,5"), ObjectIdentifier("analog-value,1"), ObjectIdentifier("notification-class,3")]
        return list(app.recipients)

    async def request(a, apdu, timeout=None):
        if app.refuse_write:
            return Error(service_choice=ConfirmedServiceChoice.writeProperty, errorClass="property", errorCode="writeAccessDenied")
        app.writes += 1
        app.recipients = list(apdu.propertyValue.cast_out(bacnet_alarms_list_type()))
        return SimpleAckPDU()

    monkeypatch.setattr(bacnet_alarms, "read_present_value", read)
    monkeypatch.setattr(bacnet_alarms, "request", request)
    return app


def bacnet_alarms_list_type():
    from bacpypes3.basetypes import Destination
    from bacpypes3.constructeddata import ListOf

    return ListOf(Destination)


def test_notification_classes_come_from_the_object_list(nc):
    assert asyncio.run(bacnet_alarms.notification_classes(nc, "10.0.0.5", 5)) == ["notification-class,3"]


def test_ensure_recipient_adds_once_and_remove_takes_it_out(nc):
    assert asyncio.run(bacnet_alarms.ensure_recipient(nc, "10.0.0.5", "notification-class,3")) is True
    assert asyncio.run(bacnet_alarms.ensure_recipient(nc, "10.0.0.5", "notification-class,3")) is False
    assert nc.writes == 1 and len(nc.recipients) == 1
    destination = nc.recipients[0]
    assert int(destination.processIdentifier) == bacnet_alarms.ALARM_PROCESS_ID
    assert bool(destination.issueConfirmedNotifications) and list(destination.transitions) == [1, 1, 1]
    assert asyncio.run(bacnet_alarms.remove_recipient(nc, "10.0.0.5", "notification-class,3")) is True
    assert nc.recipients == []
    assert asyncio.run(bacnet_alarms.remove_recipient(nc, "10.0.0.5", "notification-class,3")) is False


def test_other_recipients_are_kept(nc):
    from bacpypes3.basetypes import Destination

    other = Destination(validDays=[1] * 7, fromTime=(0, 0, 0, 0), toTime=(23, 59, 59, 99),
                        recipient=Recipient(device=ObjectIdentifier("device,77")), processIdentifier=1,
                        issueConfirmedNotifications=False, transitions=[1, 0, 1])
    nc.recipients = [other]
    asyncio.run(bacnet_alarms.ensure_recipient(nc, "10.0.0.5", "notification-class,3"))
    asyncio.run(bacnet_alarms.remove_recipient(nc, "10.0.0.5", "notification-class,3"))
    assert len(nc.recipients) == 1 and nc.recipients[0].recipient.device == ObjectIdentifier("device,77")


def test_a_refused_write_is_an_alarm_error(nc):
    nc.refuse_write = True
    with pytest.raises(bacnet_alarms.AlarmError, match="refused"):
        asyncio.run(bacnet_alarms.ensure_recipient(nc, "10.0.0.5", "notification-class,3"))


def test_a_routed_device_gets_our_device_identifier():
    app = types.SimpleNamespace(device_object=types.SimpleNamespace(objectIdentifier=ObjectIdentifier("device,999")),
                                objectIdentifier={})
    assert bacnet_alarms.our_recipient(app, "2:5").device == ObjectIdentifier("device,999")


def test_get_event_information_pages(monkeypatch):
    pages = [
        GetEventInformationACK(listOfEventSummaries=[_summary("analog-value,1")], moreEvents=True),
        GetEventInformationACK(listOfEventSummaries=[_summary("analog-value,2")], moreEvents=False),
    ]
    asked_after = []

    async def request(app, apdu, timeout=None):
        asked_after.append(apdu.lastReceivedObjectIdentifier)
        return pages.pop(0)

    monkeypatch.setattr(bacnet_alarms, "request", request)
    events = asyncio.run(bacnet_alarms.get_event_information(None, "10.0.0.5"))
    assert [e["object"] for e in events] == ["analog-value,1", "analog-value,2"]
    assert events[0]["event_state"] == "high-limit" and events[0]["acked"]["to_offnormal"] is False
    assert asked_after == [None, ObjectIdentifier("analog-value,1")]


def _summary(objid):
    return GetEventInformationEventSummary(
        objectIdentifier=ObjectIdentifier(objid), eventState="high-limit", acknowledgedTransitions=[0, 1, 1],
        eventTimeStamps=[TimeStamp(sequenceNumber=1)] * 3, notifyType="alarm", eventEnable=[1, 1, 1],
        eventPriorities=[3, 3, 3],
    )


# --- the connection manager ---


class CapturingMqtt:
    def __init__(self):
        self.published = {}

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published[topic] = (json.loads(payload) if payload else None, retain)


RECORD = {"id": "c1", "device_address": "10.0.0.5:47808", "device_instance": 5, "topic_prefix": "fbf/ahu-1",
          "poll_interval": 5.0, "forward_alarms": True,
          "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}]}


def test_a_notification_publishes_on_the_points_alarm_topic_retained(tmp_path):
    mqtt = CapturingMqtt()
    manager = cm.ConnectionManager(None, mqtt, str(tmp_path / "c.json"))
    manager.connections = {"c1": RECORD, "c2": {**RECORD, "id": "c2", "topic_prefix": "fbf/other", "forward_alarms": False}}
    manager.on_event_notification(_notification(source="10.0.0.5"))  # no port in the source: same device
    payload, retain = mqtt.published["fbf/ahu-1/zone-temp/alarm"]
    assert retain and payload["point"] == "zone-temp/alarm" and payload["value"]["to_state"] == "high-limit"
    assert not any(t.startswith("fbf/other") for t in mqtt.published)  # forward_alarms off there


def test_an_alarm_on_an_unpolled_object_still_gets_through(tmp_path):
    mqtt = CapturingMqtt()
    manager = cm.ConnectionManager(None, mqtt, str(tmp_path / "c.json"))
    manager.connections = {"c1": RECORD}
    manager.on_event_notification(_notification(objid="binary-input,7"))
    assert "fbf/ahu-1/alarms/binary-input-7" in mqtt.published


def test_keep_alarms_registers_and_publishes_whats_already_active(tmp_path, monkeypatch):
    calls = []

    async def classes(app, address, instance):
        calls.append(("classes", address, instance))
        return ["notification-class,1", "notification-class,2"]

    async def ensure(app, address, nc):
        calls.append(("ensure", nc))
        return True

    async def active(app, address):
        return [{"object": "analog-value,1", "event_state": "high-limit"}]

    monkeypatch.setattr(cm.bacnet_alarms, "notification_classes", classes)
    monkeypatch.setattr(cm.bacnet_alarms, "ensure_recipient", ensure)
    monkeypatch.setattr(cm.bacnet_alarms, "get_event_information", active)
    mqtt = CapturingMqtt()

    async def run():
        manager = cm.ConnectionManager(None, mqtt, str(tmp_path / "c.json"))
        task = asyncio.create_task(manager._keep_alarms(RECORD))
        await asyncio.sleep(0.05)
        task.cancel()

    asyncio.run(run())
    assert calls == [("classes", "10.0.0.5:47808", 5), ("ensure", "notification-class,1"), ("ensure", "notification-class,2")]
    payload, _ = mqtt.published["fbf/ahu-1/zone-temp/alarm"]
    assert payload["value"]["event_state"] == "high-limit" and payload["value"]["source"] == "summary"


def test_keep_alarms_retries_with_back_off_and_never_raises(tmp_path, monkeypatch):
    attempts = []

    async def broken(app, address, instance):
        attempts.append(1)
        raise bacnet_alarms.AlarmError("no object-list")

    monkeypatch.setattr(cm.bacnet_alarms, "notification_classes", broken)
    monkeypatch.setattr(cm, "failure_backoff", lambda interval, failures: 0.01)

    async def run():
        manager = cm.ConnectionManager(None, CapturingMqtt(), str(tmp_path / "c.json"))
        task = asyncio.create_task(manager._keep_alarms(RECORD))
        await asyncio.sleep(0.1)
        assert not task.done()
        task.cancel()

    asyncio.run(run())
    assert len(attempts) >= 3


def test_deleting_or_turning_off_forward_alarms_unregisters(tmp_path, monkeypatch):
    removed = []

    async def classes(app, address, instance):
        return ["notification-class,1"]

    async def remove(app, address, nc):
        removed.append((address, nc))
        return True

    async def idle(*a, **k):
        await asyncio.sleep(3600)

    monkeypatch.setattr(cm.bacnet_alarms, "notification_classes", classes)
    monkeypatch.setattr(cm.bacnet_alarms, "remove_recipient", remove)
    monkeypatch.setattr(cm.ConnectionManager, "_keep_alarms", idle)
    monkeypatch.setattr(cm.ConnectionManager, "_poll_point", idle)

    async def run():
        manager = cm.ConnectionManager(None, CapturingMqtt(), str(tmp_path / "c.json"))
        a = await manager.create_connection("10.0.0.5", 5, "fbf/a", RECORD["points"], read_options={"forward_alarms": True})
        await manager.update_connection(a["id"], {"forward_alarms": False})
        b = await manager.create_connection("10.0.0.6", 6, "fbf/b", RECORD["points"], read_options={"forward_alarms": True})
        await manager.delete_connection(b["id"])
        await asyncio.sleep(0.05)
        await manager.delete_connection(a["id"])
        await asyncio.sleep(0.01)

    asyncio.run(run())
    assert removed == [("10.0.0.5", "notification-class,1"), ("10.0.0.6", "notification-class,1")]


def test_forward_alarms_is_validated():
    assert cm.validate_read_options({"forward_alarms": True}) == {"forward_alarms": True}
    with pytest.raises(ValueError):
        cm.validate_read_options({"forward_alarms": "yes"})


def test_moving_a_forward_alarms_connection_unregisters_at_the_old_address(tmp_path, monkeypatch):
    removed = []

    async def classes(app, address, instance):
        return ["notification-class,1"]

    async def remove(app, address, nc):
        removed.append(address)
        return True

    async def idle(*a, **k):
        await asyncio.sleep(3600)

    monkeypatch.setattr(cm.bacnet_alarms, "notification_classes", classes)
    monkeypatch.setattr(cm.bacnet_alarms, "remove_recipient", remove)
    monkeypatch.setattr(cm.ConnectionManager, "_keep_alarms", idle)
    monkeypatch.setattr(cm.ConnectionManager, "_poll_point", idle)

    async def run():
        manager = cm.ConnectionManager(None, CapturingMqtt(), str(tmp_path / "c.json"))
        a = await manager.create_connection("10.0.0.5", 5, "fbf/a", RECORD["points"], read_options={"forward_alarms": True})
        await manager.update_connection(a["id"], {"device_address": "10.0.0.50"})
        await asyncio.sleep(0.05)
        await manager.update_connection(a["id"], {"poll_interval": 9})  # same address: nothing to remove
        await asyncio.sleep(0.05)
        await manager.delete_connection(a["id"])
        await asyncio.sleep(0.05)

    asyncio.run(run())
    assert removed == ["10.0.0.5", "10.0.0.50"]
