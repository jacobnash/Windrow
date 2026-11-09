"""Windrow#21: pre-tag with openhay-tag when the installed openhay has it
(openhay#7), with provenance (engine, rule, matched tokens) on every tag
set, and the local token matcher as the fallback. openhay#7 isn't released
yet, so a fake `openhay.tag` stands in for it here; the mock_hospital check
runs only against the real thing."""

import sys
import types

import pytest

from windrow import bacnet_tagger, mock_hospital

RULES = bacnet_tagger.load_default_rules()


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(bacnet_tagger, "_engine_failed", False)
    monkeypatch.delenv("WINDROW_TAGGER", raising=False)


class FakeTag:
    """What openhay#7's binding is asked to provide (see bacnet_tagger)."""

    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def tag_point(self, label, description, units, object_type):
        self.calls.append((label, description, units, object_type))
        if self.fail:
            raise RuntimeError("lexicon failed to load")
        if label == "CHWSupTmp3":
            return {"tags": ["chilled", "water", "temp", "sensor", "leaving"], "rule": "chw-leaving-temp",
                    "tokens": ["chw", "sup", "tmp"]}
        return {"tags": {}, "rule": None, "tokens": []}

    def haystack_unit(self, units):
        return "°F" if units == "degreesFahrenheit" else None


def _install(monkeypatch, tag):
    monkeypatch.setitem(sys.modules, "openhay", types.SimpleNamespace(tag=tag))


def test_without_openhay_tag_the_local_rules_tag_and_say_so(monkeypatch):
    monkeypatch.setitem(sys.modules, "openhay", types.SimpleNamespace())  # an openhay before #7
    tags, evidence = bacnet_tagger.tag_point_with_evidence({"label": "zone-temp", "object_identifier": "analog-value,1"}, RULES)
    assert tags == {"zone": True, "air": True, "temp": True, "sensor": True}
    assert evidence == {"engine": "windrow-local", "rule": "local:zone+temp", "tokens": ["zone", "temp"]}
    assert bacnet_tagger.tagger_engine() == "windrow-local"


def test_openhay_tag_is_used_when_installed_with_its_evidence(monkeypatch):
    fake = FakeTag()
    _install(monkeypatch, fake)
    point = {"label": "CHWSupTmp3", "description": "", "units": "degreesFahrenheit", "object_identifier": "analog-input,3"}
    tags, evidence = bacnet_tagger.tag_point_with_evidence(point, RULES)
    assert tags == {"chilled": True, "water": True, "temp": True, "sensor": True, "leaving": True}
    assert evidence == {"engine": "openhay-tag", "rule": "chw-leaving-temp", "tokens": ["chw", "sup", "tmp"]}
    assert fake.calls == [("CHWSupTmp3", "", "degreesFahrenheit", "analog-input")]
    assert bacnet_tagger.provenance_tags(evidence) == {
        "windrowTagEngine": "openhay-tag", "windrowTagRule": "chw-leaving-temp", "windrowTagTokens": ["chw", "sup", "tmp"]}
    assert bacnet_tagger.haystack_unit("degreesFahrenheit") == "°F"
    # no match: no tags and no provenance, as before
    assert bacnet_tagger.tag_point_with_evidence({"label": "mystery"}, RULES) == ({}, None)
    assert bacnet_tagger.provenance_tags(None) == {}


def test_an_openhay_tag_that_fails_falls_back_to_local_for_the_rest_of_the_run(monkeypatch):
    fake = FakeTag(fail=True)
    _install(monkeypatch, fake)
    point = {"label": "zone-temp", "object_identifier": "analog-value,1"}
    assert bacnet_tagger.tag_point_with_evidence(point, RULES)[1]["engine"] == "windrow-local"
    assert bacnet_tagger.tag_point_with_evidence(point, RULES)[1]["engine"] == "windrow-local"
    assert len(fake.calls) == 1  # not asked again
    assert bacnet_tagger.tagger_engine() == "windrow-local"


def test_windrow_tagger_forces_an_engine(monkeypatch):
    _install(monkeypatch, FakeTag())
    monkeypatch.setenv("WINDROW_TAGGER", "local")
    assert bacnet_tagger.tag_point_with_evidence({"label": "CHWSupTmp3"}, RULES) == ({}, None)
    monkeypatch.setitem(sys.modules, "openhay", types.SimpleNamespace())
    monkeypatch.setenv("WINDROW_TAGGER", "openhay")
    with pytest.raises(RuntimeError, match="openhay#7"):
        bacnet_tagger.tag_point_with_evidence({"label": "zone-temp"}, RULES)


def _real_openhay_tag() -> bool:
    try:
        import openhay
    except ImportError:
        return False
    return callable(getattr(getattr(openhay, "tag", None), "tag_point", None))


@pytest.mark.skipif(not _real_openhay_tag(), reason="depends on openhay#7 (openhay.tag not in the installed openhay)")
def test_mock_hospital_cryptic_and_camel_ahu_and_vav_points_get_tags():
    """The issue's check: with openhay-tag, the cryptic (`SUP-AIR-TEM-12`)
    and camelCase (`SupplyAirTemp`) naming styles get tags on AHU and VAV
    points - the local matcher misses most of them."""
    _spec, equipment = mock_hospital.generate_site()
    styled = []
    for equip in equipment:
        if not str(equip.get("topic_prefix", equip.get("name", ""))).split("/")[-1].startswith(("ahu", "vav")):
            continue
        for point in equip["points"]:
            label = point["label"]
            if " " not in label and "_" not in label:  # cryptic (hyphens + seq) or camel
                styled.append({**point, "description": ""})  # no help from the description
    assert styled
    tagged = [p for p in styled if bacnet_tagger.tag_point_with_evidence(p, RULES)[0]]
    assert len(tagged) / len(styled) >= 0.8, f"{len(tagged)}/{len(styled)} tagged"
