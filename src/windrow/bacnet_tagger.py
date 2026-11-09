"""
Untagged BACnet driver metadata -> Haystack marker tags, with no Xeto/
Fantom/Axon toolchain involved. Same subset-match, most-specific-wins
algorithm as baler's mapping.classify_point (rules/haystack_to_brick.yaml
there) - here it runs one stage earlier, over tokens pulled from a learned
point's label/object_identifier/description instead of over Haystack tags.
The output is the exact tag dict shape haystack_source.py publishes
over MQTT (f"{prefix}/{label}/tags"), so mqtt_listener.py -> ingest_tags ->
mapping.classify_point on the Baler side needs zero changes to
consume it - it's already generic over "wherever a /tags message came from".

ponytail: token matching only (label/object_identifier/description) - units
("degrees-fahrenheit" etc.) isn't tokenized into the match set, since turning
a raw unit string into a useful signal needs a synonym table
(fahrenheit/celsius -> temp, "%rh" -> humidity) this doesn't build yet. Add
one if label/description alone turn out to under-match on real hardware.
"""

import os
import re
from pathlib import Path

import yaml

_TOKEN_SPLIT = re.compile(r"[-_,\s]+")


def load_rules(path: str) -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)["rules"]


DEFAULT_RULES = "bacnet_to_haystack_tags.yaml"


def load_default_rules() -> list[dict]:
    """The rules shipped inside the package (src/windrow/rules/), so a plain
    `pip install` has them too, not only a checkout (Windrow#8). The
    repo-root rules/ file is a symlink to this one, for older paths.
    Found next to this module rather than via importlib.resources: windrow
    is a namespace package (no __init__.py), which resources.files() only
    handles on newer Pythons than requires-python's 3.10."""
    return load_rules(str(Path(__file__).resolve().parent / "rules" / DEFAULT_RULES))


def _tokenize(point: dict) -> set[str]:
    tokens: set[str] = set()
    for field in ("label", "object_identifier", "description"):
        value = point.get(field)
        if value:
            tokens.update(t for t in _TOKEN_SPLIT.split(str(value).lower()) if t)
    return tokens


# BACnet EngineeringUnits -> Haystack unit symbols (the units database's
# names, which openhay and every Haystack client know). Keys are the
# enum name normalised: lowercase, letters/digits only, so bacpypes3's
# "degrees-fahrenheit" and the spec's degreesFahrenheit both match.
_HAYSTACK_UNITS = {
    "degreesfahrenheit": "°F", "degreescelsius": "°C", "degreeskelvin": "K", "kelvin": "K",
    "deltadegreesfahrenheit": "Δ°F", "deltadegreeskelvin": "Δ°C",
    "percent": "%", "percentrelativehumidity": "%RH", "partspermillion": "ppm",
    "pascals": "Pa", "kilopascals": "kPa", "inchesofwater": "inH₂O", "poundsforcepersquareinch": "psi",
    "cubicfeetperminute": "cfm", "literspersecond": "L/s", "cubicmetersperhour": "m³/h",
    "feetperminute": "ft/min", "usgallonsperminute": "gal/min",
    "watts": "W", "kilowatts": "kW", "kilowatthours": "kWh", "megawatts": "MW",
    "btusperhour": "BTU/h", "tonsrefrigeration": "tonref",
    "amperes": "A", "volts": "V", "hertz": "Hz", "revolutionsperminute": "rpm",
}


def haystack_unit(bacnet_units) -> str | None:
    module = _openhay_tag_module()
    if module is not None and callable(getattr(module, "haystack_unit", None)):
        try:
            unit = module.haystack_unit(bacnet_units)
        except Exception:  # noqa: BLE001 - the local table answers instead
            unit = None
        if unit:
            return unit
    return _local_haystack_unit(bacnet_units)


def _local_haystack_unit(bacnet_units) -> str | None:
    """A learned point's BACnet units -> the Haystack `unit` tag value, or
    None when there's none (noUnits) or no known equivalent."""
    if not bacnet_units:
        return None
    return _HAYSTACK_UNITS.get(re.sub(r"[^a-z0-9]", "", str(bacnet_units).lower()))


def tag_point(point: dict, rules: list[dict]) -> dict[str, bool]:
    """Direct match only - no PROJ-style fallback here (that belongs to
    mapping.classify_point, downstream, once these tags reach Baler).
    An unmatched BACnet point just gets no tags back, which is exactly
    today's starting point (nothing), not a regression."""
    return tag_point_with_evidence(point, rules)[0]


def _local_tag(point: dict, rules: list[dict]) -> tuple[dict[str, bool], dict | None]:
    """The local token matcher, with the rule it used as evidence."""
    tokens = _tokenize(point)
    direct_matches = [r for r in rules if set(r["match"]) <= tokens]
    if not direct_matches:
        return {}, None
    rule = max(direct_matches, key=lambda r: len(r["match"]))
    evidence = {"engine": "windrow-local", "rule": "local:" + "+".join(rule["match"]), "tokens": list(rule["match"])}
    return {tag: True for tag in rule["tags"]}, evidence


# --- openhay-tag (Windrow#21, depends on openhay#7) -------------------------
#
# openhay#7 adds a shared tagger to openhay: a tokenizer that splits camelCase
# and digits (`CHWSupTmp3`), a BAS abbreviation lexicon (`SAT`, `ZN-T`,
# `stpt`, `fdbk`), units as evidence, and a subset/most-specific rule matcher
# that says which rule matched on which tokens. When the installed openhay
# has it, the pre-tag is that; until then (or if it fails), the local matcher
# above, unchanged. WINDROW_TAGGER=local|openhay|auto (default) forces one.
#
# The call this expects, which openhay#7's Python binding is asked to match:
#   openhay.tag.tag_point(label=, description=, units=, object_type=)
#     -> {"tags": {name: True | value} or [marker, ...],
#         "rule": "<rule id>" or None, "tokens": ["<matched token>", ...]}
#   openhay.tag.haystack_unit(bacnet_units) -> "<unit symbol>" or None  (optional)

_engine_failed = False


def _openhay_tag_module():
    if os.environ.get("WINDROW_TAGGER", "auto").strip().lower() == "local":
        return None
    try:
        import openhay
    except ImportError:
        return None
    module = getattr(openhay, "tag", None)
    return module if callable(getattr(module, "tag_point", None)) else None


def tagger_engine() -> str:
    """Which pre-tagger is in use: "openhay-tag" or "windrow-local"."""
    return "openhay-tag" if _openhay_tag_module() is not None and not _engine_failed else "windrow-local"


def _openhay_tag(module, point: dict) -> tuple[dict, dict | None]:
    result = module.tag_point(
        label=str(point.get("label") or ""),
        description=str(point.get("description") or ""),
        units=point.get("units"),
        object_type=str(point.get("object_identifier", "")).split(",", 1)[0].strip() or None,
    )
    raw = result.get("tags") if isinstance(result, dict) else None
    if isinstance(raw, dict):
        tags = {str(k): v for k, v in raw.items()}
    elif isinstance(raw, (list, tuple, set)):
        tags = {str(k): True for k in raw}
    else:
        tags = {}
    if not tags:
        return {}, None
    tokens = result.get("tokens") or []
    evidence = {"engine": "openhay-tag", "rule": result.get("rule"),
                "tokens": [str(t) for t in tokens] if isinstance(tokens, (list, tuple)) else []}
    return tags, evidence


def tag_point_with_evidence(point: dict, rules: list[dict]) -> tuple[dict, dict | None]:
    """(tags, evidence): the pre-tag and where it came from - {"engine",
    "rule", "tokens"} - or None when nothing matched. openhay-tag when the
    installed openhay has it (openhay#7), else the local matcher; an
    openhay-tag that raises falls back to local for the rest of the run."""
    global _engine_failed
    module = None if _engine_failed else _openhay_tag_module()
    if module is not None:
        try:
            return _openhay_tag(module, point)
        except Exception as exc:  # noqa: BLE001 - a tagger bug mustn't stop a connection being created
            _engine_failed = True
            print(f"openhay-tag failed ({exc!r}); pre-tagging with the local rules from now on")
        if os.environ.get("WINDROW_TAGGER", "auto").strip().lower() == "openhay":
            raise RuntimeError("WINDROW_TAGGER=openhay but openhay-tag failed")
    elif os.environ.get("WINDROW_TAGGER", "auto").strip().lower() == "openhay":
        raise RuntimeError("WINDROW_TAGGER=openhay but the installed openhay has no openhay.tag (openhay#7)")
    return _local_tag(point, rules)


def provenance_tags(evidence: dict | None) -> dict:
    """Evidence as tags on the point (Windrow#21), so Baler can tell a
    rule-tagged point from a guessed one: windrowTagEngine, windrowTagRule,
    windrowTagTokens. Nothing when no rule matched."""
    if not evidence:
        return {}
    out = {"windrowTagEngine": evidence.get("engine")}
    if evidence.get("rule"):
        out["windrowTagRule"] = str(evidence["rule"])
    if evidence.get("tokens"):
        out["windrowTagTokens"] = list(evidence["tokens"])
    return out
