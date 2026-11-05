"""
Generates a hospital-wing-sized mock BACnet site (~500-1000 points across
~50-80 equipment), scaled from the equipment/tagging mix in Project
Haystack's public "charlie" example (project-haystack.org/example/charlie:
30 VAVs, 6 meters, 2 chillers, 2 AHUs, 2 heating units, pumps) - not a
literal copy of charlie's ~200 points, a bigger site built the same way.

Two subcommands sharing one deterministic (seeded) generator, so nothing
needs to be persisted between them:

  python -m windrow.mock_hospital generate --out hospital_site_spec.json
      writes the BACnet object list windrow.mock_device --site-spec reads.

  python -m windrow.mock_hospital provision --device-address IP:PORT --device-instance N
      (after mock_device.py and windrow.api are both already running) regenerates
      the same equipment/point layout and POSTs one /connections call per
      equipment to windrow.api - the same call a human would make by hand, just
      scripted ~80 times. This is the only place equipment identity
      (topic_prefix) and messy point naming are decided; windrow's own
      connection_manager/bacnet_tagger/mqtt_sink do everything downstream
      unchanged.

Messiness (deliberate, not a bug): point names are rendered in one of four
styles per point (clean spaced, clean underscored, cryptic abbreviated
hyphens, or camelCase-squashed-into-one-token), each independently getting a
description or not. Chiller/boiler/pump/meter point vocab uses real BAS
abbreviations ("sup"/"ret" not "supply"/"return", "cmd", "fdbk") that never
match fbf/rules/bacnet_to_haystack_tags.yaml on purpose - those equipment
kinds, plus every equip-level class besides ahu/vav, have zero rule coverage
today, so they're exactly the "what kind of equipment is this" cases
baler's LLM auto-tag fallback exists to handle. See the implementation
plan (llm-proposed rules, area 3) for how that's supposed to close the gap
over time.
"""

import argparse
import json
import random
import time
import urllib.request

SEED = 42

# (token list, behavior, units, (min, max) or None for binary) per point kind.
# behavior -> object_type: sensor/setpoint -> analogValue, status/command -> binaryValue.
AHU_POINTS = [
    (["discharge", "air", "temp"], "sensor", "degreesFahrenheit", (55, 65)),
    (["return", "air", "temp"], "sensor", "degreesFahrenheit", (68, 75)),
    (["mixed", "air", "temp"], "sensor", "degreesFahrenheit", (60, 70)),
    (["outside", "air", "temp"], "sensor", "degreesFahrenheit", (30, 90)),
    (["supply", "fan", "status"], "status", None, None),
    (["supply", "fan", "cmd"], "command", None, None),
    (["static", "pressure"], "sensor", "inchesOfWater", (0.5, 2.0)),
]

VAV_POINTS = [
    (["zone", "air", "temp"], "sensor", "degreesFahrenheit", (68, 75)),
    (["zone", "temp", "occ", "sp"], "setpoint", "degreesFahrenheit", (70, 72)),
    (["zone", "temp", "unocc", "sp"], "setpoint", "degreesFahrenheit", (60, 65)),
    (["damper", "cmd"], "command", None, None),
    (["damper", "pos"], "sensor", "percent", (0, 100)),
    (["zone", "airflow"], "sensor", "cubicFeetPerMinute", (50, 500)),
]
VAV_OPTIONAL_POINTS = [
    (["zone", "co2"], "sensor", "partsPerMillion", (400, 1200), 0.4),
    (["zone", "humidity"], "sensor", "percentRelativeHumidity", (30, 60), 0.3),
]

CHILLER_POINTS = [
    (["chw", "sup", "temp"], "sensor", "degreesFahrenheit", (42, 46)),
    (["chw", "ret", "temp"], "sensor", "degreesFahrenheit", (54, 58)),
    (["cond", "wtr", "temp"], "sensor", "degreesFahrenheit", (75, 90)),
    (["chiller", "run", "status"], "status", None, None),
    (["chiller", "start", "cmd"], "command", None, None),
    (["kw", "input"], "sensor", "kilowatts", (50, 400)),
]

BOILER_POINTS = [
    (["hw", "sup", "temp"], "sensor", "degreesFahrenheit", (140, 180)),
    (["hw", "ret", "temp"], "sensor", "degreesFahrenheit", (120, 150)),
    (["boiler", "run", "status"], "status", None, None),
    (["boiler", "start", "cmd"], "command", None, None),
    (["firing", "rate"], "sensor", "percent", (0, 100)),
]

PUMP_POINTS = [
    (["pump", "run", "status"], "status", None, None),
    (["pump", "start", "cmd"], "command", None, None),
    (["pump", "spd", "fdbk"], "sensor", "percent", (0, 100)),
]

METER_POINTS = [
    (["kw", "demand"], "sensor", "kilowatts", (10, 500)),
    (["kwh", "total"], "sensor", "kilowattHours", (10000, 999999)),
]

EF_POINTS = [
    (["fan", "status"], "status", None, None),
    (["fan", "cmd"], "command", None, None),
]

# (equip type prefix, count, fixed point list, optional point list)
EQUIP_TEMPLATE = [
    ("ahu", 5, AHU_POINTS, []),
    ("vav", 58, VAV_POINTS, VAV_OPTIONAL_POINTS),
    ("chiller", 3, CHILLER_POINTS, []),
    ("boiler", 2, BOILER_POINTS, []),
    ("chw-pump", 3, PUMP_POINTS, []),
    ("hw-pump", 3, PUMP_POINTS, []),
    ("meter", 6, METER_POINTS, []),
    ("ef", 8, EF_POINTS, []),
]

_LABEL_STYLES = ["verbose", "underscore", "cryptic", "camel"]


def _abbrev(token: str) -> str:
    return token[:3]


def _render_label(tokens: list[str], style: str, seq: int) -> str:
    if style == "verbose":
        return " ".join(t.capitalize() for t in tokens)
    if style == "underscore":
        return "_".join(tokens)
    if style == "camel":
        return "".join(t.capitalize() for t in tokens)
    return "-".join(_abbrev(t).upper() for t in tokens) + f"-{seq}"


def _messy_name(tokens: list[str], seq: int, rng: random.Random) -> tuple[str, str]:
    label = _render_label(tokens, rng.choice(_LABEL_STYLES), seq)
    description = " ".join(tokens).capitalize() if rng.random() < 0.7 else ""
    return label, description


class _InstanceCounters:
    def __init__(self):
        self.analog = 0
        self.binary = 0

    def next(self, object_type: str) -> int:
        if object_type == "binaryValue":
            self.binary += 1
            return self.binary
        self.analog += 1
        return self.analog


def _object_identifier_str(object_type: str, instance: int) -> str:
    # Matches bacpypes3's own str(ObjectIdentifier(...)) form, confirmed
    # against windrow/tests/test_discovery.py ("analog-value,1"/"binary-value,1") -
    # not the camelCase constructor spelling used in the site-spec below.
    kind = "binary-value" if object_type == "binaryValue" else "analog-value"
    return f"{kind},{instance}"


def generate_site(seed: int = SEED) -> tuple[list[dict], list[dict]]:
    """Returns (site_spec, equipment) - site_spec is the flat BACnet object
    list windrow.mock_device consumes; equipment is a list of
    {topic_prefix, points} ready for ConnectionManager.create_connection,
    one dict per piece of equipment. Deterministic for a given seed, so
    `generate` and `provision` (run as separate processes, possibly minutes
    apart) always agree without persisting anything in between."""
    rng = random.Random(seed)
    counters = _InstanceCounters()
    site_spec: list[dict] = []
    equipment: list[dict] = []
    # bacpypes3's Application requires globally-unique object names, not
    # just unique (type, instance) pairs - and several equipment share the
    # same point-kind vocabulary (every AHU has an "Outside Air Temp"), so
    # two of them independently rolling the same "verbose" style collide.
    # Disambiguate on collision only, so uniqueness never affects tag
    # matching for the (overwhelming majority of) names that don't collide.
    used_names: set[str] = set()

    for prefix, count, fixed_points, optional_points in EQUIP_TEMPLATE:
        for i in range(1, count + 1):
            topic_prefix = f"fbf/{prefix}-{i}"
            conn_points = []
            point_defs = list(fixed_points) + [
                (tokens, behavior, units, value_range)
                for tokens, behavior, units, value_range, fraction in optional_points
                if rng.random() < fraction
            ]
            for tokens, behavior, units, value_range in point_defs:
                object_type = "binaryValue" if behavior in ("status", "command") else "analogValue"
                instance = counters.next(object_type)
                label, description = _messy_name(tokens, instance, rng)
                if label in used_names:
                    label = f"{label}-{instance}"
                used_names.add(label)
                initial_value = (
                    round(rng.uniform(*value_range), 2) if value_range is not None else rng.choice(["active", "inactive"])
                )
                # ponytail: deliberately seed 2 points already outside the
                # demo's fault-rule bands (60-80F zone temp, 50-70F
                # discharge temp - see seed_derivations_and_faults.py),
                # so a `cur`/`range` fault is visible within seconds of
                # the first reading instead of only-maybe happening
                # during a random walk over a short demo session. Not
                # representative of real equipment failure rates.
                if topic_prefix == "fbf/vav-1" and tokens == ["zone", "air", "temp"]:
                    initial_value = 92.0
                elif topic_prefix == "fbf/ahu-1" and tokens == ["discharge", "air", "temp"]:
                    initial_value = 78.0
                site_spec.append({
                    "object_type": object_type,
                    "instance": instance,
                    "object_name": label,
                    "description": description,
                    "units": units,
                    "initial_value": initial_value,
                    "behavior": behavior,
                })
                # The "verbose" style's spaces are fine as BACnet metadata
                # (object_name above) but this same string also becomes an
                # MQTT topic segment and, from there, a urn:point:... URI -
                # raw spaces aren't valid there (confirmed live: rdflib's
                # n3() serializer rejects them). Real bridges commonly
                # slugify a display name for topic use the same way;
                # tokenization for tagging is unaffected either way, since
                # bacnet_tagger splits on underscores same as spaces.
                topic_label = label.replace(" ", "_")
                conn_points.append({
                    "object_identifier": _object_identifier_str(object_type, instance),
                    "label": topic_label,
                    "description": description,
                })
            equipment.append({"topic_prefix": topic_prefix, "points": conn_points})

    return site_spec, equipment


def _provision(equipment: list[dict], api_url: str, device_address: str, device_instance: int, poll_interval: float) -> None:
    for equip in equipment:
        body = json.dumps({
            "device_address": device_address,
            "device_instance": device_instance,
            "topic_prefix": equip["topic_prefix"],
            "points": equip["points"],
            "poll_interval": poll_interval,
        }).encode("utf-8")
        req = urllib.request.Request(f"{api_url}/connections", data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req) as resp:
            resp.read()
        print(f"provisioned {equip['topic_prefix']} ({len(equip['points'])} points)")
        # Real device panels don't all answer read_property_multiple in the
        # same instant either - a small stagger avoids opening ~90
        # connections' worth of poll tasks against the mock device in the
        # same event-loop tick.
        time.sleep(0.05)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=SEED)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write the BACnet object list for windrow.mock_device --site-spec")
    gen.add_argument("--out", default="hospital_site_spec.json")

    prov = sub.add_parser("provision", help="POST one /connections call per equipment to a running windrow.api")
    prov.add_argument("--api-url", default="http://localhost:8001")
    prov.add_argument("--device-address", default="192.168.128.63:47808", help="must match the running mock_device.py's --address")
    prov.add_argument("--device-instance", type=int, default=3456, help="must match the running mock_device.py's --instance")
    prov.add_argument("--poll-interval", type=float, default=5.0)

    args = parser.parse_args()
    site_spec, equipment = generate_site(args.seed)
    point_count = sum(len(e["points"]) for e in equipment)

    if args.command == "generate":
        with open(args.out, "w") as f:
            json.dump(site_spec, f, indent=2)
        print(f"wrote {args.out}: {len(site_spec)} objects across {len(equipment)} equipment")
        return

    print(f"provisioning {len(equipment)} equipment, {point_count} points, against {args.device_address}")
    _provision(equipment, args.api_url, args.device_address, args.device_instance, args.poll_interval)


if __name__ == "__main__":
    main()
