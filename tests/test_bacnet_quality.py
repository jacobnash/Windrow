"""Windrow#20: a reading's status-flags and reliability published as its
`quality`, read in the same RPM as present-value or taken from the COV
notification. No broker or device needed: fakes stand in for both."""

import asyncio
import json
import os
import tempfile
import types

import pytest
from bacpypes3.apdu import ReadPropertyMultipleACK
from bacpypes3.basetypes import (
    ErrorType,
    PropertyIdentifier,
    ReadAccessResult,
    ReadAccessResultElement,
    ReadAccessResultElementChoice,
    Reliability,
    StatusFlags,
)
from bacpypes3.constructeddata import Any
from bacpypes3.local.device import (
    DeviceObject,  # noqa: F401 - registers standard object classes
)
from bacpypes3.primitivedata import ObjectIdentifier, Real
from bacpypes3.vendor import get_vendor_info

from windrow import bacnet_batch, bacnet_cov, mqtt_sink
from windrow import connection_manager as cm
from windrow.bacnet_read import MismatchedResponse, Sample, read_present_values
from windrow.bacnet_values import quality_of

# --- quality_of: BACnet's words, nothing when nothing is wrong ---


def test_a_healthy_point_has_no_quality():
    assert quality_of(StatusFlags([0, 0, 0, 0]), Reliability("noFaultDetected")) is None
    assert quality_of(None, None) is None
    assert quality_of([0, 0, 0, 0]) is None


def test_flags_and_reliability_are_reported_by_their_bacnet_names():
    assert quality_of(StatusFlags([0, 1, 0, 1]), Reliability("noSensor")) == {
        "status_flags": ["fault", "out-of-service"], "reliability": "no-sensor"}
    assert quality_of(StatusFlags([1, 0, 1, 0])) == {"status_flags": ["in-alarm", "overridden"]}
    # a fault reliability with the flags clear is still said
    assert quality_of([0, 0, 0, 0], "over-range") == {"reliability": "over-range"}


# --- read_present_values(status=...): the same RPM, three properties ---


class RpmApp:
    def __init__(self, answer):
        self.answer = answer
        self.requests = []

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    async def parse_property_reference(self, prop, vendor_info=None):
        return types.SimpleNamespace(propertyIdentifier=PropertyIdentifier(prop), propertyArrayIndex=None)

    async def request(self, req):
        self.requests.append(req)
        return self.answer(req)


ERROR = "error"


def _element(prop, value):
    if value == ERROR:
        choice = ReadAccessResultElementChoice(propertyAccessError=ErrorType(errorClass="property", errorCode="unknownProperty"))
    else:
        choice = ReadAccessResultElementChoice(propertyValue=Any(value))
    return ReadAccessResultElement(propertyIdentifier=PropertyIdentifier(prop), readResult=choice)


def _ack(objects: dict):
    """{objid: {prop: bacpypes3 value | ERROR}} -> a ReadPropertyMultipleACK."""
    return ReadPropertyMultipleACK(listOfReadAccessResults=[
        ReadAccessResult(objectIdentifier=ObjectIdentifier(objid),
                         listOfResults=[_element(prop, value) for prop, value in props.items()])
        for objid, props in objects.items()
    ])


def _rpm(app, objids, status):
    return asyncio.run(read_present_values(app, "10.0.0.5", objids, status=status))


def test_status_objects_ask_three_properties_and_answer_with_a_sample():
    app = RpmApp(lambda req: _ack({
        "analog-input,1": {"present-value": Real(-40.0), "status-flags": StatusFlags([0, 1, 0, 0]),
                           "reliability": Reliability("noSensor")},
        "analog-input,2": {"present-value": Real(71.5), "status-flags": StatusFlags([0, 0, 0, 0]),
                           "reliability": Reliability("noFaultDetected")},
        "analog-value,3": {"present-value": Real(55.0)},
    }))
    result = _rpm(app, ["analog-input,1", "analog-input,2", "analog-value,3"], status=["analog-input,1", "analog-input,2"])
    assert result == {
        "analog-input,1": Sample(-40.0, {"status_flags": ["fault"], "reliability": "no-sensor"}),
        "analog-input,2": Sample(71.5, None),
        "analog-value,3": 55.0,  # not asked for status: a bare value, as before
    }
    specs = app.requests[0].listOfReadAccessSpecs
    assert [len(s.listOfPropertyReferences) for s in specs] == [3, 3, 1]
    assert len(app.requests) == 1


def test_an_object_without_reliability_still_gets_its_value_and_flags():
    app = RpmApp(lambda req: _ack({
        "analog-value,1": {"present-value": Real(1.0), "status-flags": StatusFlags([0, 0, 1, 0]), "reliability": ERROR},
    }))
    assert _rpm(app, ["analog-value,1"], status=["analog-value,1"]) == {
        "analog-value,1": Sample(1.0, {"status_flags": ["overridden"]})}


def test_a_failed_value_is_a_failed_read_whatever_its_status_says():
    app = RpmApp(lambda req: _ack({
        "analog-input,9": {"present-value": ERROR, "status-flags": StatusFlags([0, 1, 0, 0])},
    }))
    assert isinstance(_rpm(app, ["analog-input,9"], status=["analog-input,9"])["analog-input,9"], ErrorType)


def test_status_for_an_object_that_didnt_ask_for_it_is_a_mismatch():
    app = RpmApp(lambda req: _ack({
        "analog-input,1": {"present-value": Real(1.0), "status-flags": StatusFlags([0, 0, 0, 0])},
    }))
    with pytest.raises(MismatchedResponse):
        _rpm(app, ["analog-input,1"], status=[])


def test_status_makes_fewer_objects_fit_an_apdu():
    assert bacnet_batch.points_per_request(480, status=True) < bacnet_batch.points_per_request(480)
    assert bacnet_batch.points_per_request(480, status=True) == 13
    assert bacnet_batch.points_per_request(None, status=True) == bacnet_batch.MAX_RPM_POINTS


# --- ReadBatcher: status asked only for the reads that want it ---


def test_the_batcher_asks_status_for_reads_that_want_it():
    calls = []

    async def read_many(app, address, objids, status=()):
        calls.append((list(objids), list(status)))
        return {o: Sample(1.0, None) if o in status else 1.0 for o in objids}

    async def read_one(app, address, objid):
        return 1.0

    slots = asyncio.Semaphore(4)
    batcher = bacnet_batch.ReadBatcher(None, "10.0.0.5", lambda: slots, read_one, read_many, cm.Counter())

    async def run():
        return await asyncio.gather(batcher.read("av,1", status=True), batcher.read("av,2"))

    assert asyncio.run(run()) == [Sample(1.0, None), 1.0]
    assert calls == [(["av,1", "av,2"], ["av,1"])]


# --- the poll loop: quality on the envelope, and a change in it is news ---


class NullMqtt:
    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        pass


def _poll(tmp_path, monkeypatch, samples, record_extra=None, point_extra=None, seconds=0.35):
    """Poll one point whose RPM answers come from `samples` in turn (the
    last repeats); returns [(value, quality)] as published."""
    answers = list(samples)
    asked_status = []

    async def read_many(app, address, objids, status=()):
        asked_status.append(list(status))
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        return {o: answer if o in status else (answer.value if isinstance(answer, Sample) else answer) for o in objids}

    monkeypatch.setattr(cm, "read_present_values", read_many)
    published = []

    def publish(client, prefix, label, value, quality=None, **_):
        published.append((value, quality))

    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", publish)

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": "10.0.0.5", "topic_prefix": "fbf/t", "poll_interval": 0.05,
                  "points": [{"object_identifier": "analog-input,1", "label": "zone-temp", **(point_extra or {})},
                             {"object_identifier": "analog-input,2", "label": "other"}],
                  **(record_extra or {})}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(seconds)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    return published, asked_status


FAULT = {"status_flags": ["fault"], "reliability": "no-sensor"}


def test_a_faulted_point_publishes_its_quality_and_a_healthy_one_none(tmp_path, monkeypatch):
    published, asked = _poll(tmp_path, monkeypatch, [Sample(-40.0, FAULT)])
    assert (-40.0, FAULT) in published
    assert all(q == FAULT for _, q in published)
    assert asked and all(sorted(s) == ["analog-input,1", "analog-input,2"] for s in asked)

    published, _ = _poll(tmp_path, monkeypatch, [Sample(71.0, None)])
    assert published and all(q is None for _, q in published)


def test_a_quality_change_at_the_same_value_is_published_despite_cov_policy(tmp_path, monkeypatch):
    # his_collect_cov: only a change in value is published - but a sensor
    # failing (or recovering) at the same value is a change worth sending
    published, _ = _poll(
        tmp_path, monkeypatch,
        [Sample(70.0, None), Sample(70.0, None), Sample(70.0, FAULT), Sample(70.0, FAULT), Sample(70.0, None)],
        point_extra={"his_collect_cov": True}, seconds=0.5,
    )
    mine = [p for p in published if p[0] == 70.0]
    # the 'other' point publishes the same stream; take zone-temp's share
    assert (70.0, FAULT) in mine
    qualities = [q for _, q in mine]
    assert qualities.index(FAULT) > 0 and None in qualities[qualities.index(FAULT):]


def test_read_quality_false_asks_and_publishes_no_status(tmp_path, monkeypatch):
    published, asked = _poll(tmp_path, monkeypatch, [Sample(-40.0, FAULT)], record_extra={"read_quality": False})
    assert published and all(q is None for _, q in published)
    assert asked and all(s == [] for s in asked)


def test_read_quality_is_a_validated_read_option():
    assert cm.validate_read_options({"read_quality": False}) == {"read_quality": False}
    with pytest.raises(ValueError, match="read_quality"):
        cm.validate_read_options({"read_quality": "no"})


# --- COV: status-flags rides in the notification beside present-value ---


class CovApp:
    def __init__(self):
        self._cov_contexts = {}

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)


def _notify(subscription, *props):
    """What bacpypes3's do_UnconfirmedCOVNotificationRequest does: put()
    each value of the notification in turn, then carry on."""

    async def run():
        await subscription._resolve()
        for prop, value in props:
            await subscription.put(types.SimpleNamespace(propertyIdentifier=PropertyIdentifier(prop), value=Any(value)))
        await asyncio.sleep(0)

    asyncio.run(run())


def test_a_cov_notification_hands_over_value_and_status_once():
    got = []
    sub = bacnet_cov.Subscription(CovApp(), "10.0.0.5", "analog-input,1", lambda v, q: got.append((v, q)))
    _notify(sub, ("present-value", Real(-40.0)), ("status-flags", StatusFlags([0, 1, 0, 0])))
    assert got == [(-40.0, {"status_flags": ["fault"]})]

    got.clear()
    _notify(sub, ("status-flags", StatusFlags([0, 0, 0, 0])), ("present-value", Real(70.0)))
    assert got == [(70.0, None)]


def test_status_without_a_value_is_not_pinned_on_the_next_value():
    got = []
    sub = bacnet_cov.Subscription(CovApp(), "10.0.0.5", "analog-input,1", lambda v, q: got.append((v, q)))
    _notify(sub, ("status-flags", StatusFlags([0, 1, 0, 0])))
    _notify(sub, ("present-value", Real(70.0)))
    assert got == [(70.0, None)]


# --- the envelope: a fourth key, only when there's something to say ---


class FakeClient:
    def __init__(self, connected=True):
        self._connected = connected
        self.published = []

    def is_connected(self):
        return self._connected

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, json.loads(payload)))


@pytest.fixture
def spool_file(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "spool.jsonl")
        monkeypatch.setenv("WINDROW_SPOOL_FILE", path)
        yield path


def test_the_envelope_carries_quality_only_when_given(spool_file):
    client = FakeClient()
    mqtt_sink.publish_reading(client, "fbf/a", "zone-temp", -40.0, ts=1.0, quality=FAULT)
    mqtt_sink.publish_reading(client, "fbf/a", "zone-temp", 70.0, ts=2.0)
    mqtt_sink.publish_reading(client, "fbf/a", "zone-temp", 70.0, ts=3.0, quality=None)
    assert [p for _, p in client.published] == [
        {"point": "zone-temp", "value": -40.0, "ts": 1.0, "quality": FAULT},
        {"point": "zone-temp", "value": 70.0, "ts": 2.0},  # byte-for-byte the pre-#20 envelope
        {"point": "zone-temp", "value": 70.0, "ts": 3.0},
    ]


def test_a_spooled_reading_keeps_its_quality_through_replay(spool_file):
    client = FakeClient(connected=False)
    mqtt_sink.publish_reading(client, "fbf/a", "zone-temp", -40.0, ts=1.0, quality=FAULT)
    mqtt_sink.publish_reading(client, "fbf/a", "zone-temp", 70.0, ts=2.0)
    client._connected = True
    mqtt_sink._flush_spool(client)
    assert [p for _, p in client.published] == [
        {"point": "zone-temp", "value": -40.0, "ts": 1.0, "quality": FAULT},
        {"point": "zone-temp", "value": 70.0, "ts": 2.0},
    ]
