"""hszinc_tags: tag export from pyhaystack grids (haxall_mqtt_bridge)."""

import datetime
import json

import hszinc

from windrow.hszinc_tags import extract_tags, to_jsonable


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
