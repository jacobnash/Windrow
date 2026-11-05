import datetime
import json
import time

import hszinc
import mqtt_test_util
import pytest

from windrow.haxall_client import connect
from windrow.haystack_bridge import extract_tags, publish_equip_tags, run_read_mode, run_write_mode, slug, to_jsonable

HAXALL_URI = "http://localhost:8180/"
HAXALL_USER = "su"
HAXALL_PASS = "windrow-demo-pass"


def test_slug_lowercases_and_hyphenates():
    assert slug("Zone Temp") == "zone-temp"
    assert slug("Windrow Write Demo") == "windrow-write-demo"


def test_to_jsonable_marker_and_singletons():
    assert to_jsonable(hszinc.MARKER) is True
    assert to_jsonable(hszinc.NA) == "na"
    assert to_jsonable(hszinc.REMOVE) == "remove"


def test_to_jsonable_ref_uses_display_name_falling_back_to_bare_id():
    assert to_jsonable(hszinc.Ref("320d231c-ebba3c2e", "AHU-1")) == "AHU-1"
    assert to_jsonable(hszinc.Ref("320d231c-ebba3c2e")) == "320d231c-ebba3c2e"


def test_to_jsonable_uri_xstr_coord():
    assert to_jsonable(hszinc.Uri("http://example.com")) == "http://example.com"
    assert to_jsonable(hszinc.XStr("Type", "data")) == {"encoding": "Type", "data": "data"}
    assert to_jsonable(hszinc.Coordinate(37.5, -122.3)) == {"lat": 37.5, "lng": -122.3}


def test_to_jsonable_date_time_datetime():
    assert to_jsonable(datetime.date(2024, 1, 1)) == "2024-01-01"
    assert to_jsonable(datetime.time(9, 51, 27)) == "09:51:27"
    assert to_jsonable(datetime.datetime(2024, 1, 1, 9, 51, 27)) == "2024-01-01T09:51:27"


def test_to_jsonable_nested_dict_and_list():
    assert to_jsonable({"a": hszinc.MARKER, "b": [1, hszinc.Uri("x")]}) == {"a": True, "b": [1, "x"]}


def test_to_jsonable_scalars_passthrough():
    assert to_jsonable(True) is True
    assert to_jsonable(42.0) == 42.0
    assert to_jsonable("hi") == "hi"
    assert to_jsonable(None) is None


def test_to_jsonable_unknown_kind_falls_back_to_str_instead_of_crashing():
    class Mystery:
        def __str__(self):
            return "mystery-value"

    assert to_jsonable(Mystery()) == "mystery-value"


def test_extract_tags_carries_every_real_tag_generically():
    row = {
        "zone": hszinc.MARKER, "air": hszinc.MARKER, "temp": hszinc.MARKER, "sensor": hszinc.MARKER,
        "point": hszinc.MARKER, "cur": hszinc.MARKER,
        "dis": "Zone Temp", "kind": "Number", "unit": "°F",
        "equipRef": hszinc.Ref("320d231c-ebba3c2e", "AHU-1"),
    }
    tags = extract_tags(row)
    assert tags == {
        "zone": True, "air": True, "temp": True, "sensor": True,
        "point": True, "cur": True,
        "dis": "Zone Temp", "kind": "Number", "unit": "°F",
        "equipRef": "AHU-1",
    }
    json.dumps(tags)  # must always be publishable as-is


def test_extract_tags_drops_grid_padding_none_values():
    """readAll() over multiple points returns one grid whose columns are the
    union across all rows - a row is padded with None for columns that
    belong to OTHER rows in the same grid (confirmed live). That None must
    never be exported as if it were a real empty-valued tag."""
    row = {
        "temp": hszinc.MARKER, "air": hszinc.MARKER, "sensor": hszinc.MARKER,
        "zone": None, "fan": None, "damper": None,  # padding from sibling rows
    }
    assert extract_tags(row) == {"temp": True, "air": True, "sensor": True}


def test_extract_tags_excludes_operational_fields():
    row = {
        "dis": "Zone Temp", "kind": "Number", "curVal": 71.0, "curStatus": "ok",
        "id": hszinc.Ref("abc"), "mod": datetime.datetime.now(),
    }
    tags = extract_tags(row)
    assert tags == {"dis": "Zone Temp", "kind": "Number"}


@pytest.fixture
def session():
    return connect(HAXALL_URI, HAXALL_USER, HAXALL_PASS)


@pytest.mark.integration
def test_read_mode_publishes_values_and_tags(session):
    pub_client = mqtt_test_util.connect()
    sub_client = mqtt_test_util.subscriber_client()
    received = {}
    sub_client.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
    sub_client.connect("localhost", 1883)
    sub_client.subscribe("fbf/haxall-readback/#")

    run_read_mode(session, pub_client, "fbf/haxall-readback", interval=0, once=True)

    value_topic = "fbf/haxall-readback/zone-temp"
    tags_topic = "fbf/haxall-readback/zone-temp/tags"
    for _ in range(20):
        sub_client.loop(timeout=0.1)
        if value_topic in received and tags_topic in received:
            break
    sub_client.disconnect()

    assert value_topic in received
    assert isinstance(received[value_topic]["value"], float)
    tags = json.loads(received[tags_topic]["value"])
    # the full real tag set, not just the 4 domain markers - proves this
    # isn't the old hardcoded whitelist anymore (unit/kind/dis/point/cur
    # were never exported before; a real Haxall Zone Temp point carries them)
    assert tags["zone"] is True
    assert tags["air"] is True
    assert tags["temp"] is True
    assert tags["sensor"] is True
    assert tags["point"] is True
    assert tags["equipRef"] == "AHU-1"
    assert tags["unit"] == "°F"
    assert tags["dis"] == "Zone Temp"
    assert "curVal" not in tags  # live state, not a static tag
    assert "id" not in tags

    # equip-side half of hasPoint/isPointOf linking - one equip/tags
    # message per real equip/site rec, keyed by its own Haystack ref
    # (not known ahead of time here, so scan for the shape rather than
    # asserting an exact topic).
    equip_tag_topics = [t for t in received if "/equip/" in t and t.endswith("/tags")]
    assert equip_tag_topics
    ahu1_tags = next(json.loads(received[t]["value"]) for t in equip_tag_topics if json.loads(received[t]["value"]).get("dis") == "AHU-1")
    assert ahu1_tags["ahu"] is True


@pytest.mark.integration
def test_publish_equip_tags_publishes_one_message_per_equip_and_site(session):
    client = mqtt_test_util.connect()
    sub_client = mqtt_test_util.subscriber_client()
    received = {}
    sub_client.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
    sub_client.connect("localhost", 1883)
    sub_client.subscribe("fbf/haxall-readback-equip/#")

    errors = publish_equip_tags(session, client, "fbf/haxall-readback-equip")
    assert errors == 0

    for _ in range(20):
        sub_client.loop(timeout=0.1)
        if any("/equip/" in t for t in received):
            break
    sub_client.disconnect()

    equip_tag_topics = [t for t in received if "/equip/" in t and t.endswith("/tags")]
    assert len(equip_tag_topics) >= 2  # at least AHU-1's equip rec and the site rec, per docs/haystack-tagging-model.md
    tag_payloads = [json.loads(received[t]["value"]) for t in equip_tag_topics]
    assert any(tags.get("ahu") is True for tags in tag_payloads)


@pytest.mark.integration
def test_write_mode_readback_matches_written_value(session):
    client = mqtt_test_util.connect()
    readback = run_write_mode(session, client, "fbf/haxall-readback", 55.5)
    assert readback == 55.5
