"""Learning a big device (Baler#139 follow-up): one ReadPropertyMultiple
for every object came back "apdu-too-long" from the mock hospital, so the
read is batched, and a batch that's still too long is halved. No device:
a fake Application answers."""

import asyncio

import pytest
from bacpypes3.apdu import AbortPDU
from bacpypes3.primitivedata import ObjectIdentifier

from windrow import discovery


class FakeApp:
    def __init__(self, n, max_objects):
        self.objects = [("analog-value", i) for i in range(1, n + 1)]
        self.max_objects = max_objects
        self.requests = []

    async def read_property(self, address, objid, prop):
        return self.objects

    async def read_property_multiple(self, address, parameter_list):
        objs = parameter_list[0::2]
        self.requests.append(len(objs))
        if len(objs) > self.max_objects:
            abort = AbortPDU(reason="apduTooLong")
            raise abort
        out = []
        for obj in objs:
            for prop in discovery.LEARN_PROPERTIES:
                value = f"pt-{obj[1]}" if prop == "object-name" else None
                out.append((obj, prop, None, value))
        return out


def test_big_device_is_read_in_batches():
    app = FakeApp(40, max_objects=16)
    points = asyncio.run(discovery.learn_points(app, "10.0.0.5", 1))
    assert [p["label"] for p in points] == [f"pt-{i}" for i in range(1, 41)]
    assert max(app.requests) <= discovery.LEARN_BATCH


def test_a_batch_that_is_still_too_long_is_halved():
    app = FakeApp(20, max_objects=3)
    points = asyncio.run(discovery.learn_points(app, "10.0.0.5", 1))
    assert len(points) == 20 and str(points[0]["object_identifier"]) == str(ObjectIdentifier(("analog-value", 1)))


def test_a_device_that_cant_answer_even_one_is_a_plain_error():
    app = FakeApp(2, max_objects=0)
    with pytest.raises(RuntimeError, match="aborted the point read"):
        asyncio.run(discovery.learn_points(app, "10.0.0.5", 1))
