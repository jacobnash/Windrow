"""Point writes (BACnet WriteProperty with priority) and the routing tags
that let Baler address them - against a fake bacpypes3 Application, so no
network. A live round trip against windrow.mock_device is exercised by hand
(see docs/command-channel.md); the mock's commandable objects are checked
here too."""

import asyncio
import json
import time

import pytest
from bacpypes3.primitivedata import Null, Real

from windrow import api, mock_device
from windrow import connection_manager as cm


class FakeApp:
    def __init__(self):
        self.values = {}
        self.writes = []

    async def write_property(self, address, objid, prop, value, priority=None):
        self.writes.append((address, objid, prop, value, priority))
        self.values[objid] = None if isinstance(value, Null) else value

    async def read_property(self, address, objid, prop):
        return self.values.get(objid, 0.0)


class FakeMqtt:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))

        class _R:
            rc = 0

        return _R()


POINTS = [
    {"label": "zone-sp", "object_identifier": "analog-value,1", "object_name": "zone sp"},
    {"label": "fan", "object_identifier": "binary-output,2", "object_name": "fan cmd"},
    {"label": "dat", "object_identifier": "analog-input,3", "object_name": "discharge air temp"},
]


def _manager(tmp_path, site="site-a"):
    m = cm.ConnectionManager(FakeApp(), FakeMqtt(), str(tmp_path / "c.json"), site_id=site, instance_id="i1" if site else None)
    m.connections["c1"] = {"id": "c1", "device_address": "10.0.0.5", "device_instance": 1,
                           "topic_prefix": "fbf/hq/ahu-1", "poll_interval": 5, "credential": None, "points": POINTS}
    return m


def test_write_analog_at_priority_publishes_read_back(tmp_path):
    m = _manager(tmp_path)
    out = asyncio.run(m.write_point("fbf/hq/ahu-1/zone-sp", 72.5, 8))
    assert m.app.writes == [("10.0.0.5", "analog-value,1", "present-value", 72.5, 8)]
    assert out["present_value"] == 72.5
    topic, payload, _ = m.mqtt_client.published[-1]
    assert topic == "fbf/hq/ahu-1/zone-sp" and json.loads(payload)["value"] == 72.5


def test_binary_values_and_relinquish(tmp_path):
    m = _manager(tmp_path)
    asyncio.run(m.write_point("fbf/hq/ahu-1/fan", True, 8))
    asyncio.run(m.write_point("fbf/hq/ahu-1/fan", "off", 8))
    asyncio.run(m.write_point("fbf/hq/ahu-1/fan", None, 8))
    values = [w[3] for w in m.app.writes]
    assert values[:2] == ["active", "inactive"] and isinstance(values[2], Null)


@pytest.mark.parametrize("topic, value, priority, msg", [
    ("fbf/hq/ahu-1/dat", 1.0, 8, "not commandable"),       # analog-input
    ("fbf/hq/ahu-1/nope", 1.0, 8, "no connected point"),
    ("fbf/other/zone-sp", 1.0, 8, "no connected point"),
    ("fbf/hq/ahu-1/zone-sp", 1.0, 0, "1-17"),
    ("fbf/hq/ahu-1/zone-sp", 1.0, 18, "1-17"),
    ("fbf/hq/ahu-1/zone-sp", None, 17, "not relinquished"),
    ("fbf/hq/ahu-1/zone-sp", "warm", 8, "analog"),
    ("fbf/hq/ahu-1/fan", "maybe", 8, "binary"),
])
def test_rejected_writes_never_reach_the_device(tmp_path, topic, value, priority, msg):
    m = _manager(tmp_path)
    with pytest.raises(cm.PointWriteError, match=msg):
        asyncio.run(m.write_point(topic, value, priority))
    assert m.app.writes == []


def test_level_17_writes_relinquish_default(tmp_path):
    m = _manager(tmp_path)
    asyncio.run(m.write_point("fbf/hq/ahu-1/zone-sp", 68.0, 17))
    assert m.app.writes == [("10.0.0.5", "analog-value,1", "relinquish-default", 68.0, None)]


def test_expired_command_is_not_applied(tmp_path):
    m = _manager(tmp_path)
    with pytest.raises(cm.PointWriteError, match="expired"):
        asyncio.run(m.write_point("fbf/hq/ahu-1/zone-sp", 70.0, 8, expires_at=time.time() - 1))
    assert m.app.writes == []
    asyncio.run(m.write_point("fbf/hq/ahu-1/zone-sp", 70.0, 8, expires_at=time.time() + 30))
    assert len(m.app.writes) == 1


def test_equip_tags_advertise_command_route_and_writable_points(tmp_path):
    m = _manager(tmp_path)
    m._publish_equip_membership("fbf/hq/ahu-1", POINTS)
    m._tag_points("fbf/hq/ahu-1", POINTS)
    raw = {t: p for t, p, _ in m.mqtt_client.published}
    assert '\\"windrowSite\\": \\"site-a\\"' in raw["fbf/hq/ahu-1/equip/tags"] \
        or '"windrowSite": "site-a"' in raw["fbf/hq/ahu-1/equip/tags"]
    assert "i1" in raw["fbf/hq/ahu-1/equip/tags"]
    assert "writable" in raw["fbf/hq/ahu-1/zone-sp/tags"] and "writable" in raw["fbf/hq/ahu-1/fan/tags"]
    assert "writable" not in raw["fbf/hq/ahu-1/dat/tags"]


def test_no_route_tags_without_site_id(tmp_path):
    m = _manager(tmp_path, site=None)
    m._publish_equip_membership("fbf/hq/ahu-1", POINTS)
    assert "windrowSite" not in m.mqtt_client.published[-1][1]


def test_command_channel_point_write_echoes_request_id(tmp_path):
    m = _manager(tmp_path)
    loop = asyncio.new_event_loop()
    import threading

    threading.Thread(target=loop.run_forever, daemon=True).start()
    try:
        on_command = api._make_on_command(m, loop, "site-a", "i1")
        replies = FakeMqtt()

        class Msg:
            topic = "cmd/site-a/i1/point-write"
            payload = json.dumps({"action": "point_write", "id": "req-42", "topic": "fbf/hq/ahu-1/zone-sp",
                                  "value": 71.0, "priority": 8}).encode()

        on_command(replies, None, Msg())
        topic, payload, _ = replies.published[-1]
        out = json.loads(payload)
        assert topic == "cmd/site-a/i1/result"
        assert out["ok"] and out["id"] == "req-42" and out["action"] == "point_write"
        assert out["result"]["present_value"] == 71.0
    finally:
        loop.call_soon_threadsafe(loop.stop)


def test_mock_device_objects_are_commandable():
    async def go():
        obj = mock_device._make_object({"object_type": "analogValue", "instance": 9, "object_name": "sp",
                                        "initial_value": 70.0, "units": None})
        await obj.write_property("presentValue", Real(72.0), priority=8)
        obj.presentValue = 65.0  # a simulation tick writes priority 16
        assert obj.presentValue == 72.0
        await obj.write_property("presentValue", Null(()), priority=8)
        assert obj.presentValue == 65.0

    asyncio.run(go())
