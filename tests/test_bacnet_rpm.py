"""Baler#127: ReadPropertyMultiple batching per device, with fallback to
single reads, and COV subscriptions with a slow poll underneath. No broker
or device needed - the live versions are in test_bacnet_rpm_live.py."""

import asyncio
import types

import pytest
from bacpypes3.apdu import (
    AbortPDU,
    ConfirmedServiceChoice,
    Error,
    ReadPropertyMultipleACK,
    RejectPDU,
    SimpleAckPDU,
)
from bacpypes3.basetypes import (
    ErrorType,
    PropertyIdentifier,
    ReadAccessResult,
    ReadAccessResultElement,
    ReadAccessResultElementChoice,
)
from bacpypes3.constructeddata import Any
from bacpypes3.local.device import (
    DeviceObject,  # noqa: F401 - registers standard object classes
)
from bacpypes3.primitivedata import ObjectIdentifier, Real
from bacpypes3.vendor import get_vendor_info

from windrow import bacnet_batch, bacnet_cov
from windrow import connection_manager as cm
from windrow.bacnet_read import (
    MismatchedResponse,
    ReadError,
    RPMUnsupported,
    read_present_values,
)

# --- read_present_values: one RPM, every answer checked against the question ---


class RpmApp:
    """Answers a ReadPropertyMultiple with `answer(request)`."""

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


def _ack(values: dict):
    """{objid: float | "error"} -> a ReadPropertyMultipleACK for present-value."""
    results = []
    for objid, value in values.items():
        if value == "error":
            choice = ReadAccessResultElementChoice(propertyAccessError=ErrorType(errorClass="object", errorCode="unknownObject"))
        else:
            choice = ReadAccessResultElementChoice(propertyValue=Any(Real(value)))
        results.append(ReadAccessResult(
            objectIdentifier=ObjectIdentifier(objid),
            listOfResults=[ReadAccessResultElement(propertyIdentifier=PropertyIdentifier("present-value"), readResult=choice)],
        ))
    return ReadPropertyMultipleACK(listOfReadAccessResults=results)


RPM = ConfirmedServiceChoice.readPropertyMultiple


def _rpm(app, objids, timeout=None):
    return asyncio.run(read_present_values(app, "10.0.0.5", objids, timeout=timeout))


def test_rpm_returns_each_value_under_the_callers_identifier():
    app = RpmApp(lambda req: _ack({"analog-value,1": 70.5, "analog-value,2": 55.0}))
    assert _rpm(app, ["analog-value,1", "analog-value,2"]) == {"analog-value,1": 70.5, "analog-value,2": 55.0}
    assert len(app.requests) == 1 and len(app.requests[0].listOfReadAccessSpecs) == 2


def test_rpm_per_object_error_is_that_objects_result_only():
    app = RpmApp(lambda req: _ack({"analog-value,1": 70.5, "analog-value,9": "error"}))
    result = _rpm(app, ["analog-value,1", "analog-value,9"])
    assert result["analog-value,1"] == 70.5
    assert isinstance(result["analog-value,9"], ErrorType)


def test_rpm_answer_for_an_object_nobody_asked_for_is_rejected():
    # Baler#61's wrong-point value, RPM edition: a late answer to a reused invoke ID
    app = RpmApp(lambda req: _ack({"analog-value,1": 70.5, "analog-input,376": 86116.5}))
    with pytest.raises(MismatchedResponse, match="analog-input,376"):
        _rpm(app, ["analog-value,1", "analog-value,2"])


def test_rpm_reject_unrecognized_service_means_no_rpm():
    app = RpmApp(lambda req: RejectPDU(reason="unrecognizedService"))
    with pytest.raises(RPMUnsupported) as info:
        _rpm(app, ["analog-value,1", "analog-value,2"])
    assert info.value.too_big is False


def test_rpm_abort_segmentation_means_too_big():
    app = RpmApp(lambda req: AbortPDU(reason="segmentationNotSupported"))
    with pytest.raises(RPMUnsupported) as info:
        _rpm(app, ["analog-value,1", "analog-value,2"])
    assert info.value.too_big is True


def test_rpm_services_error_means_no_rpm_other_errors_are_read_errors():
    app = RpmApp(lambda req: Error(service_choice=RPM, errorClass="services", errorCode="serviceRequestDenied"))
    with pytest.raises(RPMUnsupported):
        _rpm(app, ["analog-value,1", "analog-value,2"])
    app = RpmApp(lambda req: Error(service_choice=RPM, errorClass="device", errorCode="operationalProblem"))
    with pytest.raises(ReadError):
        _rpm(app, ["analog-value,1", "analog-value,2"])


def test_rpm_that_never_returns_times_out():
    class Hanging(RpmApp):
        def request(self, req):
            return asyncio.get_event_loop().create_future()

    with pytest.raises(TimeoutError):
        _rpm(Hanging(None), ["analog-value,1", "analog-value,2"], timeout=0.05)


def test_points_per_request_follows_the_devices_apdu_size():
    assert bacnet_batch.points_per_request(None) == bacnet_batch.MAX_RPM_POINTS
    assert bacnet_batch.points_per_request(1476) == bacnet_batch.MAX_RPM_POINTS  # capped
    assert bacnet_batch.points_per_request(480) == 23  # a typical MS/TP device
    assert bacnet_batch.points_per_request(50) == 2
    assert bacnet_batch.points_per_request(10) == 1


# --- ReadBatcher: one RPM per tick per device, and the fallbacks ---


class Device:
    """A fake device behind read_one/read_many, counting requests."""

    def __init__(self, rpm="ok", values=None):
        self.rpm = rpm  # "ok" | "reject" | "too_big" | "timeout" | "boom"
        self.values = values or {}
        self.single_reads = []
        self.rpm_sizes = []

    async def read_one(self, app, address, objid):
        self.single_reads.append(objid)
        if self.rpm == "timeout":
            raise TimeoutError()
        return self.values.get(objid, 1.0)

    async def read_many(self, app, address, objids):
        self.rpm_sizes.append(len(objids))
        if self.rpm == "reject":
            raise RPMUnsupported("rejected")
        if self.rpm == "too_big" and len(objids) > 2:
            raise RPMUnsupported("too big", too_big=True)
        if self.rpm == "timeout":
            raise TimeoutError()
        if self.rpm == "boom":
            raise MismatchedResponse("late answer")
        return {o: self.values.get(o, 1.0) for o in objids}


def _batcher(device, max_points=50):
    stats = cm.Counter()
    slots = asyncio.Semaphore(4)
    return bacnet_batch.ReadBatcher(None, "10.0.0.5", lambda: slots, device.read_one, device.read_many, stats,
                                    max_points=max_points), stats


def _read_all(batcher, objids):
    async def run():
        return await asyncio.gather(*(batcher.read(o) for o in objids), return_exceptions=True)

    return asyncio.run(run())


def test_reads_asked_together_go_out_as_one_rpm_per_max_points():
    device = Device(values={"av,1": 5.0})
    batcher, stats = _batcher(device, max_points=10)
    results = _read_all(batcher, ["av,1"] + [f"av,{i}" for i in range(2, 26)])
    assert results[0] == 5.0 and len(results) == 25
    assert device.rpm_sizes == [10, 10, 5]
    assert device.single_reads == []
    assert stats["read_property_multiple"] == 3


def test_the_same_object_from_two_connections_is_asked_once():
    device = Device()
    batcher, _ = _batcher(device)
    assert _read_all(batcher, ["av,1", "av,1", "av,2"]) == [1.0, 1.0, 1.0]
    assert device.rpm_sizes == [2]


def test_a_device_that_rejects_rpm_is_read_singly_from_then_on():
    device = Device(rpm="reject")
    batcher, stats = _batcher(device)
    assert _read_all(batcher, ["av,1", "av,2", "av,3"]) == [1.0, 1.0, 1.0]
    assert batcher.rpm_supported is False
    _read_all(batcher, ["av,1", "av,2"])
    assert device.rpm_sizes == [3]  # never asked again
    assert stats["read_property"] == 5


def test_an_answer_too_big_halves_the_batch_and_reads_this_one_singly():
    device = Device(rpm="too_big")
    batcher, _ = _batcher(device, max_points=8)
    assert _read_all(batcher, [f"av,{i}" for i in range(8)]) == [1.0] * 8
    assert batcher.max_points == 4 and batcher.rpm_supported is True
    assert len(device.single_reads) == 8


def test_a_timeout_fails_the_whole_flush_without_fanning_out():
    device = Device(rpm="timeout")
    batcher, _ = _batcher(device, max_points=2)
    results = _read_all(batcher, ["av,1", "av,2", "av,3", "av,4"])
    assert all(isinstance(r, TimeoutError) for r in results)
    assert device.rpm_sizes == [2]  # the second chunk wasn't even sent
    assert device.single_reads == []


def test_a_mismatched_answer_is_re_asked_one_at_a_time():
    device = Device(rpm="boom")
    batcher, _ = _batcher(device)
    assert _read_all(batcher, ["av,1", "av,2"]) == [1.0, 1.0]
    assert sorted(device.single_reads) == ["av,1", "av,2"]
    assert batcher.rpm_supported is True


# --- the poll loop: requests per tick, per device ---


class NullMqtt:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append(topic)


def _site(device_count=1, connections_per_device=20, points_per_connection=10, interval=0.05):
    records = []
    for d in range(device_count):
        for c in range(connections_per_device):
            records.append({
                "id": f"d{d}c{c}", "device_address": f"10.0.0.{d + 1}", "device_instance": d, "topic_prefix": f"fbf/d{d}/c{c}",
                "poll_interval": interval,
                "points": [{"object_identifier": f"analog-value,{c * 100 + p}", "label": f"p{p}"} for p in range(points_per_connection)],
            })
    return records


def test_a_device_with_many_connections_is_polled_with_a_few_rpms_per_tick(tmp_path, monkeypatch):
    """The acceptance bar: requests per poll cycle drop at least 10x. 200
    points over 20 connections on one device, 50 per RPM -> 4 requests a
    tick instead of 200."""
    device = Device()
    monkeypatch.setattr(cm, "read_present_value", device.read_one)
    monkeypatch.setattr(cm, "read_present_values", device.read_many)

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        records = _site()
        tasks = [asyncio.get_running_loop().create_task(manager._poll_loop(r)) for r in records]
        await asyncio.sleep(0.6)  # ~12 ticks
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return manager

    manager = asyncio.run(run())
    requests = manager.stats["read_property_multiple"] + manager.stats["read_property"]
    ticks = len(device.rpm_sizes) / 4
    assert ticks >= 5
    assert device.single_reads == []
    assert all(size == 50 for size in device.rpm_sizes)
    points_read = sum(device.rpm_sizes)
    assert points_read / requests >= 10


def test_read_multiple_false_keeps_one_read_per_point(tmp_path, monkeypatch):
    device = Device()
    monkeypatch.setattr(cm, "read_present_value", device.read_one)
    monkeypatch.setattr(cm, "read_present_values", device.read_many)

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        record = _site(connections_per_device=1)[0] | {"read_multiple": False}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    assert device.rpm_sizes == [] and len(device.single_reads) >= 10


def test_one_bad_object_in_a_batch_publishes_nothing_for_it_and_its_siblings_publish(tmp_path, monkeypatch):
    async def read_many(app, address, objids):
        return {o: (ErrorType(errorClass="object", errorCode="unknownObject") if o.endswith(",1") else 2.0) for o in objids}

    monkeypatch.setattr(cm, "read_present_values", read_many)
    published = []
    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append((label, value)))

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        record = _site(connections_per_device=1, points_per_connection=3)[0]
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    labels = {label for label, _ in published}
    assert labels == {"p0", "p2"}  # p1 is analog-value,1
    assert all(value == 2.0 for _, value in published)


# --- COV ---


class CovApp:
    """Answers SubscribeCOV per `answer`, and keeps bacpypes3's context table."""

    def __init__(self, answer="ack"):
        self.answer = answer
        self._cov_contexts = {}
        self.subscribes = 0

    async def get_vendor_info(self, device_address=None):
        return get_vendor_info(0)

    async def parse_object_identifier(self, objid, vendor_info=None):
        return ObjectIdentifier(objid)

    def request(self, req):
        future = asyncio.get_event_loop().create_future()
        if req.lifetime is None:  # a cancel
            future.set_result(SimpleAckPDU())
            return future
        self.subscribes += 1
        if self.answer == "ack":
            future.set_result(SimpleAckPDU())
        elif self.answer == "unsupported":
            future.set_result(Error(service_choice=ConfirmedServiceChoice.subscribeCOV, errorClass="object",
                                    errorCode="optionalFunctionalityNotSupported"))
        elif self.answer == "hang":
            pass  # never answered
        return future

    async def notify(self, value: float):
        """What bacpypes3's do_UnconfirmedCOVNotificationRequest does."""
        for sub in list(self._cov_contexts.values()):
            await sub.put(types.SimpleNamespace(propertyIdentifier=PropertyIdentifier("present-value"), value=Any(Real(value))))


def _cov_run(tmp_path, monkeypatch, app, seconds, record_extra=None, during=None):
    polls = []

    async def read_one(app_, address, objid):
        polls.append(asyncio.get_running_loop().time())
        return 1.0

    monkeypatch.setattr(cm, "read_present_value", read_one)
    published = []
    monkeypatch.setattr(cm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append(value))
    monkeypatch.setattr(cm.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(bacnet_cov, "READ_TIMEOUT", 0.05)

    async def run():
        manager = cm.ConnectionManager(app, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c", "device_address": "10.0.0.5", "topic_prefix": "fbf/t", "poll_interval": 0.05,
                  "subscribe_cov": True, "cov_lifetime": 60, "cov_poll_interval": 10.0,
                  "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}], **(record_extra or {})}
        task = asyncio.get_running_loop().create_task(manager._poll_loop(record))
        if during:
            await during(manager)
        else:
            await asyncio.sleep(seconds)
        stats = manager.read_stats()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        return stats

    stats = asyncio.run(run())
    return polls, published, stats


def test_a_subscribed_point_publishes_on_notification_and_polls_slowly(tmp_path, monkeypatch):
    app = CovApp("ack")

    async def during(manager):
        await asyncio.sleep(0.3)
        assert manager.read_stats()["cov_subscriptions"] == 1
        await app.notify(72.5)

    polls, published, stats = _cov_run(tmp_path, monkeypatch, app, 0, during=during)
    assert 72.5 in published  # straight from the notification, no poll in between
    # the first poll, then the slow 10 s tick (which can fall anywhere in the
    # next 10 s) - not every 0.05 s, which would be ~10 in this time
    assert len(polls) <= 3
    assert stats["cov_notifications"] == 1
    assert app._cov_contexts == {}  # unsubscribed on cancel


def test_a_point_whose_device_refuses_cov_is_polled_at_its_normal_rate(tmp_path, monkeypatch):
    app = CovApp("unsupported")
    polls, _, stats = _cov_run(tmp_path, monkeypatch, app, 0.4)
    assert app.subscribes == 1  # not retried
    assert len(polls) >= 4
    assert stats["cov_subscriptions"] == 0


def test_a_subscribe_that_never_answers_times_out_and_polling_carries_on(tmp_path, monkeypatch):
    app = CovApp("hang")
    polls, published, stats = _cov_run(tmp_path, monkeypatch, app, 0.4)
    assert len(polls) >= 4 and 1.0 in published
    assert stats["cov_subscriptions"] == 0


def test_a_point_can_opt_out_of_its_connections_cov(tmp_path, monkeypatch):
    app = CovApp("ack")
    polls, _, _ = _cov_run(tmp_path, monkeypatch, app, 0.3, record_extra={
        "points": [{"object_identifier": "analog-value,1", "label": "zone-temp", "subscribe_cov": False}],
    })
    assert app.subscribes == 0 and len(polls) >= 3


# --- read options on the record ---


def test_read_options_are_validated():
    assert cm.validate_read_options({"read_multiple": False, "read_multiple_max": 20, "subscribe_cov": True,
                                     "cov_lifetime": 120, "cov_poll_interval": 60}) == {
        "read_multiple": False, "read_multiple_max": 20, "subscribe_cov": True, "cov_lifetime": 120, "cov_poll_interval": 60}
    for bad in ({"read_multiple": "yes"}, {"read_multiple_max": 0}, {"cov_lifetime": 0}, {"cov_poll_interval": -1},
                {"subscribe_cov": 1}):
        with pytest.raises(ValueError):
            cm.validate_read_options(bad)
    with pytest.raises(ValueError, match="subscribe_cov"):
        cm.validate_points([{"object_identifier": "analog-value,1", "label": "a", "subscribe_cov": "on"}])


def test_create_and_update_store_read_options_and_cap_the_batch(tmp_path, monkeypatch):
    async def read_one(app, address, objid):
        return 1.0

    monkeypatch.setattr(cm, "read_present_value", read_one)

    async def run():
        manager = cm.ConnectionManager(None, NullMqtt(), str(tmp_path / "c.json"))
        record = await manager.create_connection("10.0.0.5", 5, "fbf/a", [{"object_identifier": "analog-value,1", "label": "a"}],
                                                 read_options={"read_multiple_max": 7})
        assert record["read_multiple_max"] == 7
        assert manager._batcher("10.0.0.5").max_points == 7
        updated = await manager.update_connection(record["id"], {"read_multiple_max": 30, "subscribe_cov": False})
        assert updated["read_multiple_max"] == 30 and updated["subscribe_cov"] is False
        assert manager._batcher("10.0.0.5").max_points == 30
        with pytest.raises(ValueError):
            await manager.update_connection(record["id"], {"cov_lifetime": "long"})
        await manager.delete_connection(record["id"])

    asyncio.run(run())


# --- bacpypes3 raises Error/Reject/Abort, as BaseExceptions ---


class Raising(RpmApp):
    """What bacpypes3 really does: app.request raises the Error/Reject/Abort
    (which derive from BaseException, not Exception) instead of returning it."""

    async def request(self, req):
        self.requests.append(req)
        raise self.answer(req)


def test_a_raised_reject_still_means_no_rpm():
    with pytest.raises(RPMUnsupported):
        _rpm(Raising(lambda req: RejectPDU(reason="unrecognizedService")), ["analog-value,1", "analog-value,2"])


def test_a_raised_error_on_a_single_read_is_returned_as_a_failed_read():
    from windrow.bacnet_read import read_present_value

    app = Raising(lambda req: Error(service_choice=ConfirmedServiceChoice.readProperty, errorClass="object",
                                    errorCode="unknownObject"))
    answer = asyncio.run(read_present_value(app, "10.0.0.5", "analog-value,99"))
    assert isinstance(answer, Error)


def test_a_raised_cov_refusal_is_cov_unsupported():
    class RaisingCov(CovApp):
        def request(self, req):
            future = asyncio.get_event_loop().create_future()
            future.set_exception(Error(service_choice=ConfirmedServiceChoice.subscribeCOV, errorClass="object",
                                       errorCode="optionalFunctionalityNotSupported"))
            return future

    async def run():
        sub = bacnet_cov.Subscription(RaisingCov(), "10.0.0.5", "analog-value,1", lambda v: None)
        with pytest.raises(bacnet_cov.CovUnsupported):
            await sub.subscribe(60)

    asyncio.run(run())
