"""Baler#80/#82: a Modbus connection publishes the same retained tags a
BACnet one does - per-point {prefix}/{label}/tags and one
{prefix}/equip/tags naming its points - on create and on restore, so Baler
gives it an equipment record. No device or broker: the poll loop is
stubbed out and publishes go to a fake MQTT client."""

import asyncio
import json

import pytest

from windrow import connection_store
from windrow import modbus_connection_manager as mcm


class FakeMqttClient:
    def __init__(self):
        self.published: dict[str, dict] = {}

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=True):
        assert retain, "tags must be retained, like BACnet's"
        if payload == b"":  # mqtt_sink.clear_retained
            self.published.pop(topic, None)
            return
        self.published[topic] = json.loads(json.loads(payload)["value"])


@pytest.fixture
def manager(tmp_path, monkeypatch):
    client = FakeMqttClient()
    mgr = mcm.ModbusConnectionManager(mqtt_client=client, state_file=str(tmp_path / "m.json"))

    async def no_poll(record):
        mgr.tasks[record["id"]] = None

    monkeypatch.setattr(mgr, "_start_poll_task", no_poll)
    return mgr, client


def test_create_publishes_point_tags_and_equip_membership(manager):
    mgr, client = manager
    asyncio.run(mgr.create_connection(
        host="192.168.128.230", topic_prefix="fbf/home/modbus",
        points={"zone_temp": 0, "fan_status": 1, "reg_7": 7}, units={"zone_temp": "°F"},
    ))
    assert client.published["fbf/home/modbus/equip/tags"] == {"tags": {}, "points": ["zone_temp", "fan_status", "reg_7"]}
    assert client.published["fbf/home/modbus/zone_temp/tags"] == {
        "zone": True, "air": True, "temp": True, "sensor": True, "unit": "°F"}
    assert client.published["fbf/home/modbus/fan_status/tags"] == {"fan": True, "run": True, "sensor": True}
    assert client.published["fbf/home/modbus/reg_7/tags"] == {}


def test_restored_connections_publish_their_tags(manager, tmp_path):
    """Connections made before this fix get their equipment on restart."""
    mgr, client = manager
    connection_store.save(mgr.state_file, [{
        "id": "c1", "host": "10.0.0.9", "port": 502, "device_id": 1, "topic_prefix": "fbf/old",
        "poll_interval": 5.0, "points": {"zone_temp": 0}, "credential": None,
    }])
    asyncio.run(mgr.start())
    assert client.published["fbf/old/equip/tags"] == {"tags": {}, "points": ["zone_temp"]}
    assert "unit" not in client.published["fbf/old/zone_temp/tags"]


@pytest.mark.parametrize("units", [["°F"], {"nope": "°F"}, {"zone_temp": ""}, {"zone_temp": 3}])
def test_bad_units_are_rejected(manager, units):
    mgr, _ = manager
    with pytest.raises(ValueError):
        asyncio.run(mgr.create_connection(host="h", topic_prefix="fbf/x", points={"zone_temp": 0}, units=units))


def test_update_republishes_tags_and_keeps_surviving_units(manager):
    """An edit (Windrow#2) republishes tags for the new shape: units of
    removed points drop, a moved prefix clears the old equip record."""
    mgr, client = manager
    rec = asyncio.run(mgr.create_connection(
        host="10.0.0.9", topic_prefix="fbf/home/modbus",
        points={"zone_temp": 0, "fan_status": 1}, units={"zone_temp": "°F"},
    ))
    out = asyncio.run(mgr.update_connection(rec["id"], {"points": {"zone_temp": 0}, "topic_prefix": "fbf/home/mb2"}))
    assert out["units"] == {"zone_temp": "°F"}
    assert "fbf/home/modbus/equip/tags" not in client.published
    assert "fbf/home/modbus/zone_temp/tags" not in client.published
    assert client.published["fbf/home/mb2/equip/tags"] == {"tags": {}, "points": ["zone_temp"]}
    assert client.published["fbf/home/mb2/zone_temp/tags"]["unit"] == "°F"

    out = asyncio.run(mgr.update_connection(rec["id"], {"units": {}}))
    assert "units" not in out
    assert "unit" not in client.published["fbf/home/mb2/zone_temp/tags"]
    with pytest.raises(ValueError):
        asyncio.run(mgr.update_connection(rec["id"], {"units": {"nope": "°F"}}))


def test_point_objects_publish_unit_writable_and_site_route(tmp_path, monkeypatch):
    """Baler#128: a point object's own unit and writable flag reach its
    tags, and with the command channel on the equipment record names the
    site/instance Baler routes its pointWrite to."""
    client = FakeMqttClient()
    mgr = mcm.ModbusConnectionManager(client, str(tmp_path / "m.json"), site_id="site-a", instance_id="i1")

    async def no_poll(record):
        mgr.tasks[record["id"]] = None

    monkeypatch.setattr(mgr, "_start_poll_task", no_poll)
    asyncio.run(mgr.create_connection(host="h", topic_prefix="fbf/x", points={
        "zone_temp_sp": {"register": 4, "type": "int16", "scale": 0.1, "unit": "°F", "writable": True},
        "discharge_air_temp": {"register": 2, "type": "float32"},
    }, units={"discharge_air_temp": "°C"}))
    assert client.published["fbf/x/zone_temp_sp/tags"]["unit"] == "°F"
    assert client.published["fbf/x/zone_temp_sp/tags"]["writable"] is True
    assert client.published["fbf/x/discharge_air_temp/tags"]["unit"] == "°C"
    assert "writable" not in client.published["fbf/x/discharge_air_temp/tags"]
    assert client.published["fbf/x/equip/tags"]["tags"] == {"windrowSite": "site-a", "windrowInstance": "i1"}


@pytest.mark.parametrize("points, units", [
    ({"t": {"register": 1, "unit": "°F"}}, {"t": "°C"}),  # unit in two places
    ({"t": {"register": 1, "type": "double"}}, None),
    ({"t": "1"}, None),
    ({"t#": 1}, None),
    ({}, None),
])
def test_bad_points_are_rejected_at_create(manager, points, units):
    mgr, _ = manager
    with pytest.raises(ValueError):
        asyncio.run(mgr.create_connection(host="h", topic_prefix="fbf/x", points=points, units=units))
    assert mgr.connections == {}


def test_update_keeps_write_levels_only_for_surviving_writable_points(manager):
    mgr, _ = manager
    rec = asyncio.run(mgr.create_connection(host="h", topic_prefix="fbf/x", points={
        "a": {"register": 1, "writable": True}, "b": {"register": 2, "writable": True}, "c": 3,
    }, units={"a": "°F"}))
    mgr.connections[rec["id"]]["write_levels"] = {"a": {"8": 70}, "b": {"8": 1}}
    out = asyncio.run(mgr.update_connection(rec["id"], {"points": {
        "a": {"register": 1, "writable": True, "unit": "°C"}, "b": 2,
    }}))
    assert out["write_levels"] == {"a": {"8": 70}}
    assert "units" not in out  # a's unit moved onto the point; the old map entry is dropped, not a conflict
    out = asyncio.run(mgr.update_connection(rec["id"], {"points": {"b": 2}}))
    assert "write_levels" not in out
