"""
_tag_points/_publish_equip_membership in isolation - no live BACnet device,
broker, or asyncio poll task needed, just a fake mqtt client recording
publish() calls (same style as test_connection_manager_publish_policy.py's
pure-function testing of should_publish).
"""

import json

from windrow.connection_manager import ConnectionManager


class FakeMqttClient:
    def __init__(self):
        self.published: list[tuple[str, str]] = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=True):
        self.published.append((topic, payload))


def _manager(tmp_path) -> tuple[ConnectionManager, FakeMqttClient]:
    client = FakeMqttClient()
    manager = ConnectionManager(app=None, mqtt_client=client, state_file=str(tmp_path / "connections.json"))
    return manager, client


def test_tag_points_publishes_one_tags_message_per_point(tmp_path):
    manager, client = _manager(tmp_path)
    points = [
        {"object_identifier": "analog-value,1", "label": "zone-temp"},
        {"object_identifier": "binary-value,1", "label": "fan-status"},
    ]

    manager._tag_points("fbf/ahu-3", points)

    topics = {topic for topic, _ in client.published}
    assert topics == {"fbf/ahu-3/zone-temp/tags", "fbf/ahu-3/fan-status/tags"}

    payload = dict(client.published)["fbf/ahu-3/zone-temp/tags"]
    tags = json.loads(json.loads(payload)["value"])
    assert tags == {"zone": True, "air": True, "temp": True, "sensor": True}


def test_tag_points_publishes_empty_dict_for_no_match(tmp_path):
    manager, client = _manager(tmp_path)
    manager._tag_points("fbf/ahu-3", [{"object_identifier": "network-port,1", "label": "NetworkPort-1"}])

    payload = dict(client.published)["fbf/ahu-3/NetworkPort-1/tags"]
    tags = json.loads(json.loads(payload)["value"])
    assert tags == {}


def test_publish_equip_membership_lists_every_point_label(tmp_path):
    manager, client = _manager(tmp_path)
    points = [
        {"object_identifier": "analog-value,1", "label": "zone-temp"},
        {"object_identifier": "binary-value,1", "label": "fan-status"},
    ]

    manager._publish_equip_membership("fbf/ahu-3", points)

    assert len(client.published) == 1
    topic, payload = client.published[0]
    assert topic == "fbf/ahu-3/equip/tags"
    data = json.loads(json.loads(payload)["value"])
    assert data == {"tags": {}, "points": ["zone-temp", "fan-status"]}


def test_tag_points_carries_bacnet_units_as_haystack_unit(tmp_path):
    manager, client = _manager(tmp_path)
    manager._tag_points("fbf/ahu-3", [
        {"object_identifier": "analog-value,1", "label": "zone-temp", "units": "degreesFahrenheit"},
        {"object_identifier": "analog-value,2", "label": "mystery", "units": "noUnits"},
    ])
    published = dict(client.published)
    temp = json.loads(json.loads(published["fbf/ahu-3/zone-temp/tags"])["value"])
    assert temp["unit"] == "°F" and temp["temp"] is True
    assert "unit" not in json.loads(json.loads(published["fbf/ahu-3/mystery/tags"])["value"])


def test_an_explicit_unit_overrides_the_learned_units(tmp_path):
    """Baler#82: a device that reports no (or the wrong) units."""
    manager, client = _manager(tmp_path)
    manager._tag_points("fbf/p", [
        {"object_identifier": "analog-value,1", "label": "zone-temp", "unit": "°C", "units": "degrees-fahrenheit"},
        {"object_identifier": "analog-value,2", "label": "zone-humidity", "units": None, "unit": "%RH"},
    ])
    tags = {t: json.loads(json.loads(p)["value"]) for t, p in client.published}
    assert tags["fbf/p/zone-temp/tags"]["unit"] == "°C"
    assert tags["fbf/p/zone-humidity/tags"]["unit"] == "%RH"
