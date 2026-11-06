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

import re

import yaml

_TOKEN_SPLIT = re.compile(r"[-_,\s]+")


def load_rules(path: str) -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)["rules"]


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
    tokens = _tokenize(point)
    direct_matches = [r for r in rules if set(r["match"]) <= tokens]
    if not direct_matches:
        return {}
    rule = max(direct_matches, key=lambda r: len(r["match"]))
    return {tag: True for tag in rule["tags"]}
