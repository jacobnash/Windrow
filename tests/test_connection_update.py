"""update_connection on both managers - validate, persist, restart the poll
task(s) cleanly, clear retained topics a point no longer publishes to, and
(BACnet) retag. No broker or device: a fake mqtt client records publishes,
BACnet reads and the Modbus client are faked (same approach as
test_bacnet_resume.py / test_modbus_reconnect.py)."""

import asyncio
import json
from typing import ClassVar

import pytest

from windrow import connection_manager as cm
from windrow import connection_store
from windrow import modbus_connection_manager as mcm


class FakeMqttClient:
    def __init__(self):
        self.published: list[tuple[str, object, bool]] = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))

    def topics(self, payload=None):
        return [t for t, p, _ in self.published if payload is None or p == payload]


async def _wait(cond, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return True
        await asyncio.sleep(0.01)
    return False


# --- BACnet ---


@pytest.fixture
def bacnet(tmp_path, monkeypatch):
    reads = []

    async def fake_read(app, address, objid):
        reads.append((address, objid))
        return 70.0

    monkeypatch.setattr(cm, "read_present_value", fake_read)
    client = FakeMqttClient()
    mgr = cm.ConnectionManager(app=None, mqtt_client=client, state_file=str(tmp_path / "c.json"))
    return mgr, client, reads


POINTS = [
    {"object_identifier": "analog-value,1", "label": "zone-temp", "units": "degrees-fahrenheit"},
    {"object_identifier": "analog-value,2", "label": "zone-sp"},
]


def test_bacnet_update_points_restarts_polling_clears_removed_and_retags(bacnet, tmp_path):
    mgr, client, reads = bacnet

    async def run():
        record = await mgr.create_connection("10.0.0.5", 5, "fbf/ahu-1", POINTS, poll_interval=0.01)
        old_task = mgr.tasks[record["id"]]
        assert await _wait(lambda: ("10.0.0.5", "analog-value,2") in reads)
        client.published.clear()

        updated = await mgr.update_connection(record["id"], {"points": [
            {"object_identifier": "analog-value,1", "label": "zone-temp", "present_value": 70.0, "poll_interval": 0.01},
            {"object_identifier": "analog-value,3", "label": "fan-cmd"},
        ]})
        try:
            assert updated["id"] == record["id"]
            # /learn's present_value snapshot isn't stored; known fields are
            assert updated["points"][0] == {"object_identifier": "analog-value,1", "label": "zone-temp", "poll_interval": 0.01}
            assert old_task.done()
            assert mgr.tasks[record["id"]] is not old_task
            assert len(mgr.tasks) == 1
            # zone-sp is gone: its retained value and tags are deleted
            assert ("fbf/ahu-1/zone-sp", b"", True) in client.published
            assert ("fbf/ahu-1/zone-sp/tags", b"", True) in client.published
            assert "fbf/ahu-1/zone-temp" not in client.topics(b"")
            # kept and new points are (re)tagged, equip membership republished
            assert {"fbf/ahu-1/zone-temp/tags", "fbf/ahu-1/fan-cmd/tags", "fbf/ahu-1/equip/tags"} <= set(client.topics())
            equip = next(json.loads(p) for t, p, _ in client.published if t == "fbf/ahu-1/equip/tags")
            assert json.loads(equip["value"])["points"] == ["zone-temp", "fan-cmd"]
            # the new point is actually polled
            reads.clear()
            assert await _wait(lambda: ("10.0.0.5", "analog-value,3") in reads)
            assert ("10.0.0.5", "analog-value,2") not in reads
        finally:
            for task in mgr.tasks.values():
                task.cancel()

    asyncio.run(run())
    stored = connection_store.load(str(tmp_path / "c.json"))
    assert [p["label"] for p in stored[0]["points"]] == ["zone-temp", "fan-cmd"]


def test_bacnet_topic_prefix_change_clears_every_old_topic(bacnet):
    mgr, client, _ = bacnet

    async def run():
        record = await mgr.create_connection("10.0.0.5", 5, "fbf/ahu-1", POINTS, poll_interval=30)
        client.published.clear()
        updated = await mgr.update_connection(record["id"], {"topic_prefix": "fbf/ahu-2/", "poll_interval": 10})
        for task in mgr.tasks.values():
            task.cancel()
        return updated

    updated = asyncio.run(run())
    assert updated["topic_prefix"] == "fbf/ahu-2"
    assert updated["poll_interval"] == 10
    cleared = set(client.topics(b""))
    assert cleared == {
        "fbf/ahu-1/zone-temp", "fbf/ahu-1/zone-temp/tags",
        "fbf/ahu-1/zone-sp", "fbf/ahu-1/zone-sp/tags", "fbf/ahu-1/equip/tags",
    }
    assert {"fbf/ahu-2/zone-temp/tags", "fbf/ahu-2/zone-sp/tags", "fbf/ahu-2/equip/tags"} <= set(client.topics())


def test_bacnet_update_unknown_id_returns_none(bacnet):
    mgr, _, _ = bacnet
    assert asyncio.run(mgr.update_connection("nope", {"poll_interval": 1})) is None


@pytest.mark.parametrize("patch", [
    {},
    {"poll_intervall": 5},
    {"poll_interval": 0},
    {"poll_interval": True},
    {"poll_interval": "5"},
    {"topic_prefix": ""},
    {"topic_prefix": "fbf/#"},
    {"points": []},
    {"points": {"a": 1}},
    {"points": [{"label": "x"}]},
    {"points": [{"object_identifier": "analog-value,1", "label": ""}]},
    {"points": [{"object_identifier": "analog-value,1", "label": "x"}, {"object_identifier": "analog-value,2", "label": "x"}]},
    {"points": [{"object_identifier": "analog-value,1", "label": "x", "poll_interval": -1}]},
    {"points": [{"object_identifier": "analog-value,1", "label": "x", "his_collect_cov": "yes"}]},
])
def test_bacnet_update_rejects_bad_patches_and_changes_nothing(bacnet, tmp_path, patch):
    mgr, client, _ = bacnet

    async def run():
        record = await mgr.create_connection("10.0.0.5", 5, "fbf/ahu-1", POINTS, poll_interval=30)
        task = mgr.tasks[record["id"]]
        client.published.clear()
        with pytest.raises(ValueError):
            await mgr.update_connection(record["id"], patch)
        assert mgr.tasks[record["id"]] is task and not task.done()
        task.cancel()
        return record

    record = asyncio.run(run())
    assert client.published == []
    assert connection_store.load(str(tmp_path / "c.json"))[0] == record


# --- Modbus ---


class FakeModbusClient:
    instances: ClassVar[list["FakeModbusClient"]] = []

    def __init__(self, record):
        self.host, self.port = record["host"], record["port"]
        self.connected = False
        self.closed = False
        self.reads: list[tuple[int, int]] = []
        FakeModbusClient.instances.append(self)

    async def connect(self):
        self.connected = True
        return True

    async def read_holding_registers(self, address, count=1, device_id=1):
        self.reads.append((address, device_id))
        return type("R", (), {"registers": [address], "isError": lambda self: False})()

    def close(self):
        self.connected = False
        self.closed = True


@pytest.fixture
def modbus(tmp_path, monkeypatch):
    FakeModbusClient.instances = []
    published = []
    monkeypatch.setattr(mcm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append((prefix, label)))
    client = FakeMqttClient()
    mgr = mcm.ModbusConnectionManager(mqtt_client=client, state_file=str(tmp_path / "m.json"))
    monkeypatch.setattr(mgr, "_new_client", FakeModbusClient)
    return mgr, client, published


def test_modbus_update_reconnects_to_new_host_and_clears_removed_points(modbus, tmp_path):
    mgr, client, published = modbus

    async def run():
        record = await mgr.create_connection("10.0.0.6", "fbf/m1", {"a": 0, "b": 1}, poll_interval=0.01)
        old_task = mgr.tasks[record["id"]]
        assert await _wait(lambda: ("fbf/m1", "b") in published)
        old_client = mgr.clients[record["id"]]

        updated = await mgr.update_connection(
            record["id"], {"host": "10.0.0.7", "port": 1502, "device_id": 3, "points": {"a": 0, "c": 7}},
        )
        try:
            assert old_task.done() and old_client.closed
            assert updated["host"] == "10.0.0.7" and updated["port"] == 1502 and updated["device_id"] == 3
            assert await _wait(lambda: record["id"] in mgr.clients and mgr.clients[record["id"]].reads)
            new_client = mgr.clients[record["id"]]
            assert (new_client.host, new_client.port) == ("10.0.0.7", 1502)
            assert (7, 3) in new_client.reads
            assert set(client.topics(b"")) == {"fbf/m1/b", "fbf/m1/b/tags"}
            assert len(mgr.tasks) == 1
        finally:
            await mgr.close()
        return updated

    updated = asyncio.run(run())
    assert connection_store.load(str(tmp_path / "m.json"))[0] == updated


def test_modbus_topic_prefix_change_clears_all_old_points(modbus):
    mgr, client, _ = modbus

    async def run():
        record = await mgr.create_connection("10.0.0.6", "fbf/m1", {"a": 0, "b": 1}, poll_interval=30)
        updated = await mgr.update_connection(record["id"], {"topic_prefix": "fbf/m2", "poll_interval": 2.5})
        await mgr.close()
        return updated

    updated = asyncio.run(run())
    assert updated["topic_prefix"] == "fbf/m2" and updated["poll_interval"] == 2.5
    # The old prefix's equipment record goes too (its tags moved, Baler#80).
    assert set(client.topics(b"")) == {"fbf/m1/a", "fbf/m1/a/tags", "fbf/m1/b", "fbf/m1/b/tags", "fbf/m1/equip/tags"}


def test_modbus_update_unknown_id_returns_none(modbus):
    mgr, _, _ = modbus
    assert asyncio.run(mgr.update_connection("nope", {"port": 502})) is None


@pytest.mark.parametrize("patch", [
    {},
    {"unit": 1},
    {"points": {}},
    {"points": [0, 1]},
    {"points": {"a": -1}},
    {"points": {"a": "0"}},
    {"points": {"a": True}},
    {"host": ""},
    {"port": 0},
    {"port": 70000},
    {"device_id": 256},
    {"poll_interval": -2},
    {"topic_prefix": "fbf/+/x"},
])
def test_modbus_update_rejects_bad_patches(modbus, patch):
    mgr, client, _ = modbus

    async def run():
        record = await mgr.create_connection("10.0.0.6", "fbf/m1", {"a": 0}, poll_interval=30)
        task = mgr.tasks[record["id"]]
        with pytest.raises(ValueError):
            await mgr.update_connection(record["id"], patch)
        assert mgr.tasks[record["id"]] is task and not task.done()
        await mgr.close()

    asyncio.run(run())
    assert client.topics(b"") == []
