"""
bacnet_tagger.py in isolation - no live BACnet device or MQTT needed, just
token matching over plain dicts shaped like discovery.learn_points' output.
"""

import pytest

from windrow.bacnet_tagger import load_rules, tag_point


@pytest.fixture(autouse=True)
def _local_tagger(monkeypatch):
    """These pin the local rules' output; openhay-tag (Windrow#21) has its own tests."""
    monkeypatch.setenv("WINDROW_TAGGER", "local")


RULES_PATH = "rules/bacnet_to_haystack_tags.yaml"
RULES = load_rules(RULES_PATH)


def test_load_rules_returns_real_rules():
    assert len(RULES) >= 10
    assert {"zone", "air", "temp", "sensor"} in [set(r["tags"]) for r in RULES]


def test_tag_point_matches_on_label_tokens():
    point = {"object_identifier": "analog-value,1", "label": "zone-temp", "units": "degrees-fahrenheit"}
    assert tag_point(point, RULES) == {"zone": True, "air": True, "temp": True, "sensor": True}


def test_tag_point_matches_fan_status_from_learn_example():
    """Exact shape of the fbf/learn docs example - proves the tagger works
    on the API's own real response, not a synthetic fixture."""
    point = {"object_identifier": "binary-value,1", "label": "fan-status", "present_value": "active", "units": None}
    assert tag_point(point, RULES) == {"fan": True, "run": True, "sensor": True}


def test_tag_point_matches_on_description_when_label_is_uninformative():
    point = {"object_identifier": "analog-value,7", "label": "AV7", "description": "Zone Temp Setpoint"}
    assert tag_point(point, RULES) == {"zone": True, "air": True, "temp": True, "sp": True}


def test_tag_point_returns_empty_dict_for_no_match():
    point = {"object_identifier": "analog-value,99", "label": "network-port,1", "units": None}
    assert tag_point(point, RULES) == {}


def test_tag_point_prefers_most_specific_rule():
    """"zone-temp-setpoint" satisfies both the 2-token zone/temp rule and
    the 3-token zone/temp/setpoint rule - the longer match must win, same
    tie-break as baler's classify_point."""
    point = {"object_identifier": "analog-value,3", "label": "zone-temp-setpoint"}
    assert tag_point(point, RULES) == {"zone": True, "air": True, "temp": True, "sp": True}


def test_haystack_unit_from_bacnet_units_any_spelling():
    from windrow.bacnet_tagger import haystack_unit

    assert haystack_unit("degreesFahrenheit") == "°F"  # spec name (mock_hospital)
    assert haystack_unit("degrees-fahrenheit") == "°F"  # bacpypes3's enum str
    assert haystack_unit("percentRelativeHumidity") == "%RH"
    assert haystack_unit("cubic-feet-per-minute") == "cfm"
    assert haystack_unit("noUnits") is None
    assert haystack_unit(None) is None
    assert haystack_unit("furlongs-per-fortnight") is None


def test_packaged_rules_are_the_repo_rules():
    """windrow.api loads the rules from inside the package (Windrow#8); the
    repo-root rules/ path older checkouts and Dockerfiles use still works."""
    from windrow.bacnet_tagger import load_default_rules

    assert load_default_rules() == RULES


def test_built_wheel_ships_the_rules(tmp_path):
    """A plain `pip install` (what install-edge.sh does) gets the rules: they
    are in the wheel as package data, not left behind in the checkout."""
    import pathlib
    import shutil
    import subprocess
    import sys
    import zipfile

    root = pathlib.Path(__file__).resolve().parent.parent
    # Built from a copy: setuptools writes build/ and *.egg-info in the tree.
    src = tmp_path / "src-copy"
    shutil.copytree(root / "src", src / "src", ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"))
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        shutil.copy(root / name, src / name)
    subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "-q",
                    "-w", str(tmp_path), str(src)], check=True)
    (wheel,) = tmp_path.glob("windrow-*.whl")
    names = zipfile.ZipFile(wheel).namelist()
    assert "windrow/rules/bacnet_to_haystack_tags.yaml" in names
