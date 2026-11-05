"""
Bridges Windrow's real MQTT bridge output directly into Haxall's real
hisWrite - same wire envelope {point, value, ts} mqtt_listener.py
consumes. Each message becomes one hisWrite call against the point whose
windrowCur tag matches that topic, so history - via "his" (BalerHisExt)
- lands in Baler continuously, in real time, driven by real
BACnet sensor changes.

ponytail: calls hisWrite directly rather than updating curVal and relying
on hisCollectCov, because the real native connector path
(~/baler/fantom-windrow-conn/) hit an unresolved "connector never
opens on watch, even via the real watchSub op and confirmed-correct
openPin('watch') source path" limitation in this Haxall build - see the
fantom_xeto_haxall_reference memory. This bridge is the pragmatic
fallback: real live data, real Haxall hisWrite (a boot-basis function,
confirmed reliable via Axon eval all session), just not yet fully
autonomous connector-driven historization.

Also pushes each windrowPoint's real Haystack tags into Baler's
ontology graph (POST /tags) once at startup - tags are static, unlike
readings, so this doesn't belong in the per-message hot path. Point
identity has to match what hisWrite already uses (the Haxall ref, not
the MQTT topic) since that's the same urn:point:{ref} entity the
readings are landing on via BalerHisExt.
"""

import argparse
import json

import paho.mqtt.client as mqtt
import requests

from windrow.haxall_client import connect, eval_axon
from windrow.haystack_bridge import extract_tags


def ref_str(row) -> str:
    return str(row.get("id")).split("'")[0].strip().lstrip("@")


def build_topic_map(rows) -> dict:
    mapping = {}
    for row in rows:
        topic = row.get("windrowCur")
        if not topic:
            continue
        mapping[topic] = ref_str(row)
    return mapping


def push_tags(rows, baler_url: str) -> None:
    for row in rows:
        ref = ref_str(row)
        try:
            resp = requests.post(f"{baler_url}/tags", json={"point": ref, "tags": extract_tags(row)}, timeout=10)
            resp.raise_for_status()
            print(f"tagged {ref}: {resp.json()}")
        except requests.RequestException as exc:
            print(f"tag push failed for {ref}: {exc}")


def make_on_message(session, topic_map: dict):
    def on_message(client, userdata, msg):
        ref = topic_map.get(msg.topic)
        if ref is None:
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"malformed payload on {msg.topic}: {exc!r}")
            return

        value = payload.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            print(f"skipping non-numeric value on {msg.topic}: {value!r}")
            return

        expr = f"hisWrite(toGrid([{{ts: now(), val: {value}}}], {{id: @{ref}}}))"
        try:
            eval_axon(session, expr)
            print(f"{msg.topic} -> {ref}: {value}")
        except Exception as exc:
            print(f"hisWrite failed for {msg.topic}: {exc}")

    return on_message


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--haxall-uri", default="http://localhost:8280/")
    parser.add_argument("--haxall-user", default="su")
    parser.add_argument("--haxall-pass", required=True)
    parser.add_argument("--baler-url", default="http://localhost:8000")
    args = parser.parse_args()

    session = connect(args.haxall_uri, args.haxall_user, args.haxall_pass)
    rows = eval_axon(session, "readAll(windrowPoint)")
    topic_map = build_topic_map(rows)
    if not topic_map:
        raise SystemExit("no windrowPoint records with windrowCur found - create points first")
    print(f"bridging {len(topic_map)} topic(s): {list(topic_map)}")
    push_tags(rows, args.baler_url)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = make_on_message(session, topic_map)
    client.connect(args.mqtt_host, args.mqtt_port)
    for topic in topic_map:
        client.subscribe(topic)
    print(f"listening on {args.mqtt_host}:{args.mqtt_port}")
    client.loop_forever()


if __name__ == "__main__":
    main()
