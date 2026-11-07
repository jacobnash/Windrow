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
