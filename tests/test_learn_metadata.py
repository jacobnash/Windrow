"""Windrow#19: /learn captures the BACnet metadata that identifies a point
when its name doesn't - object type, commandability, state texts, range,
COV increment, structured-view membership and the device's identity - and a
connection publishes it, raw, with the point's tags."""

import asyncio
import contextlib
import json
import random
import socket

import pytest
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import DeviceObjectReference, ErrorType
from bacpypes3.constructeddata import ArrayOf
from bacpypes3.primitivedata import CharacterString, ObjectIdentifier, Real

from windrow import bacnet_tagger, discovery
from windrow import connection_manager as cm

UNKNOWN = ErrorType(errorClass="property", errorCode="unknownProperty")


class FakeApp:
    """A device whose object-list and properties come from `objects`:
    {objid text: {property: value}}; a property not given answers
    unknown-property, as a real device does inside an RPM."""

    def __init__(self, objects: dict, device: dict | None = None, device_fails=False):
        self.objects = objects
        self.device = device or {}
        self.device_fails = device_fails
        self.asked: dict[str, list[str]] = {}

    async def read_property(self, address, objid, prop):
        assert prop == "object-list"
        return [ObjectIdentifier("device,7")] + [ObjectIdentifier(o) for o in self.objects]

    async def read_property_multiple(self, address, parameter_list):
        out = []
        for obj, props in zip(parameter_list[0::2], parameter_list[1::2]):
            self.asked[str(obj)] = list(props)
            if str(obj[0]) == "device":
                if self.device_fails:
                    raise TimeoutError()
                source = self.device
            else:
                source = self.objects[str(obj)]
            for prop in props:
                out.append((obj, prop, None, source.get(prop, UNKNOWN)))
        return out


def _learn(app):
    return asyncio.run(discovery.learn_points(app, "10.0.0.5", 7))


def test_each_type_asks_its_own_extras_in_the_same_batch():
    app = FakeApp({
        "analog-input,1": {"object-name": "AI1"},
        "multi-state-value,2": {"object-name": "MSV2"},
        "binary-output,3": {"object-name": "BO3"},
        "structured-view,1": {"object-name": "AHU-1"},
    })
    _learn(app)
    assert "cov-increment" in app.asked["analog-input,1"] and "relinquish-default" not in app.asked["analog-input,1"]
    assert {"state-text", "relinquish-default"} <= set(app.asked["multi-state-value,2"])
    assert {"active-text", "inactive-text", "relinquish-default"} <= set(app.asked["binary-output,3"])
    assert app.asked["structured-view,1"] == ["object-name", "subordinate-list"]
    assert app.asked["device,7"] == discovery.DEVICE_PROPERTIES


def test_learned_points_carry_the_raw_evidence():
    app = FakeApp({
        "analog-value,1": {"object-name": "AHU1-SAT-SP", "present-value": Real(55.0), "units": "degreesFahrenheit",
                           "min-pres-value": Real(45.0), "max-pres-value": Real(65.0), "cov-increment": Real(0.1),
                           "relinquish-default": Real(55.0)},
        "analog-input,2": {"object-name": "AI-2", "present-value": Real(70.0)},
        "multi-state-value,3": {"object-name": "MODE", "present-value": 2,
                                "state-text": ArrayOf(CharacterString)(["Off", "Low", "High"])},
        "binary-input,4": {"object-name": "BI-4", "active-text": "Alarm", "inactive-text": "Normal"},
    }, device={"object-name": "AHU-1 Controller", "vendor-name": "Acme Controls", "model-name": "AC-500",
               "location": "Mech Room 2"})
    points = {p["object_identifier"]: p for p in _learn(app)}

    sp = points["analog-value,1"]
    assert sp["object_type"] == "analog-value" and sp["commandable"] is True
    assert (sp["min_pres_value"], sp["max_pres_value"], sp["cov_increment"]) == (45.0, 65.0, 0.1)
    assert sp["device"] == {"instance": 7, "name": "AHU-1 Controller", "vendor_name": "Acme Controls",
                            "model_name": "AC-500", "location": "Mech Room 2"}

    assert "commandable" not in points["analog-input,2"]  # inputs aren't asked
    assert "min_pres_value" not in points["analog-input,2"]  # not answered: left out, not None
    assert points["multi-state-value,3"]["state_text"] == ["Off", "Low", "High"]
    assert points["multi-state-value,3"]["commandable"] is False  # no relinquish-default: no priority array
    assert (points["binary-input,4"]["active_text"], points["binary-input,4"]["inactive_text"]) == ("Alarm", "Normal")


def test_structured_views_group_their_members_and_arent_points():
    app = FakeApp({
        "analog-input,1": {"object-name": "SAT"},
        "analog-input,2": {"object-name": "RAT"},
        "analog-input,3": {"object-name": "OAT"},
        "structured-view,1": {"object-name": "AHU-1", "subordinate-list": [
            DeviceObjectReference(objectIdentifier=("analog-input", 1)),
            DeviceObjectReference(objectIdentifier=("analog-input", 2)),
        ]},
        "structured-view,2": {"object-name": "Supply", "subordinate-list": [
            DeviceObjectReference(objectIdentifier=("analog-input", 1)),
        ]},
    })
    points = {p["object_identifier"]: p for p in _learn(app)}
    assert set(points) == {"analog-input,1", "analog-input,2", "analog-input,3"}
    assert points["analog-input,1"]["structured_views"] == ["AHU-1", "Supply"]
    assert points["analog-input,2"]["structured_views"] == ["AHU-1"]
    assert "structured_views" not in points["analog-input,3"]


def test_a_device_that_wont_say_who_it_is_still_has_its_points_learned():
    app = FakeApp({"analog-input,1": {"object-name": "SAT"}}, device_fails=True)
    (point,) = _learn(app)
    assert point["label"] == "SAT" and point["device"] == {"instance": 7}


# --- published with the point's tags ---


class Client:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))


def test_the_metadata_is_published_raw_with_the_points_tags(tmp_path):
    manager = cm.ConnectionManager(None, Client(), str(tmp_path / "c.json"))
    point = {
        "object_identifier": "multi-state-value,3", "label": "MODE", "object_type": "multi-state-value",
        "commandable": True, "state_text": ["Off", "Low", "High"], "structured_views": ["AHU-1"],
        "device": {"instance": 7, "name": "AHU-1 Controller", "vendor_name": "Acme Controls", "model_name": "AC-500",
                   "location": "Mech Room 2"},
    }
    manager._tag_points("fbf/ahu-1", cm.validate_points([point]))
    (topic, payload), = manager.mqtt_client.published
    assert topic == "fbf/ahu-1/MODE/tags"
    tags = json.loads(json.loads(payload)["value"])
    assert tags["bacnetObjectType"] == "multi-state-value"
    assert tags["bacnetCommandable"] is True
    assert tags["bacnetStateText"] == ["Off", "Low", "High"]
    assert tags["bacnetStructuredView"] == ["AHU-1"]
    assert tags["bacnetVendorName"] == "Acme Controls" and tags["bacnetModelName"] == "AC-500"
    assert tags["bacnetDeviceName"] == "AHU-1 Controller" and tags["bacnetLocation"] == "Mech Room 2"


def test_a_point_without_learned_metadata_gets_only_its_object_type():
    assert bacnet_tagger.raw_metadata_tags({"object_identifier": "analog-input,1", "label": "x"}) == {
        "bacnetObjectType": "analog-input"}
    # not commandable is no tag, not a false one
    assert "bacnetCommandable" not in bacnet_tagger.raw_metadata_tags(
        {"object_identifier": "analog-value,1", "commandable": False})


def test_metadata_fields_are_kept_on_the_stored_point_and_shape_checked():
    (kept,) = cm.validate_points([{"object_identifier": "analog-value,1", "label": "a", "cov_increment": 0.5,
                                   "present_value": 3.0, "device": {"vendor_name": "Acme"}}])
    assert kept["cov_increment"] == 0.5 and kept["device"] == {"vendor_name": "Acme"}
    assert "present_value" not in kept  # a snapshot, still dropped
    for bad in ({"state_text": "Off,On"}, {"cov_increment": "0.1"}, {"commandable": "yes"}, {"device": "x"},
                {"structured_views": [1]}):
        with pytest.raises(ValueError):
            cm.validate_points([{"object_identifier": "analog-value,1", "label": "a", **bad}])


# --- the real mock device, over loopback: real bacpypes3 encoding both ways ---


def _free_udp_port() -> int:
    for _ in range(20):
        port = random.randint(40000, 60000)
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    pytest.skip("no free UDP port on loopback")


def test_learn_against_the_mock_device_over_loopback():
    from bacpypes3.app import Application

    from windrow.mock_device import MockDevice

    device_port, client_port = _free_udp_port(), _free_udp_port()

    async def run():
        device_args = SimpleArgumentParser().parse_args(
            ["--address", f"127.0.0.1/32:{device_port}", "--instance", "3456", "--name", "mock-ahu"])
        mock = MockDevice(device_args)
        client = Application.from_args(SimpleArgumentParser().parse_args(
            ["--address", f"127.0.0.1/32:{client_port}", "--instance", "4194000"]))
        try:
            await asyncio.sleep(0.5)
            return await asyncio.wait_for(discovery.learn_points(client, f"127.0.0.1:{device_port}", 3456), 20)
        finally:
            client.close()
            mock.app.close()

    points = {p["label"]: p for p in asyncio.run(run())}
    zone = points["zone-temp"]
    assert zone["object_type"] == "analog-value" and zone["commandable"] is True
    assert zone["cov_increment"] == 0.1
    assert points["fan-status"]["commandable"] is True
    assert zone["device"]["name"] == "mock-ahu" and zone["device"]["vendor_name"]
