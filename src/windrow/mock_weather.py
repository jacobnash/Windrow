"""
A simulated weather station that publishes to Baler over MQTT - the
"weather" edge-node role (Baler#35). Same envelope as Baler's
scripts/publish_weather_station.py (equip/tags, <point>/tags, readings), so
Baler classifies it as a Brick Weather_Station with weather-sensor points.

Values follow a smooth daily cycle plus a little noise, so charts look
like weather rather than random numbers. Authenticates with an edge-node
broker account (Ops -> Edge nodes in Baler): the account may publish only
under its site's topic prefixes, so --topic-prefix must sit under one.
Port 8883 turns TLS on (windrow.mqtt_sink.use_tls).

    python -m windrow.mock_weather --mqtt-host baler.example.com --mqtt-port 8883 \\
        --topic-prefix fbf/home/weather --interval 60
    (MQTT_USERNAME / MQTT_PASSWORD from the environment)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time

from windrow import mqtt_sink

# label -> (Haystack weatherStation point markers + unit, mean, daily swing, noise)
POINTS = {
    "temp": ({"weather": True, "air": True, "temp": True, "sensor": True, "unit": "°F"}, 62.0, 12.0, 0.4),
    "humidity": ({"weather": True, "air": True, "humidity": True, "sensor": True, "unit": "%"}, 60.0, -18.0, 1.0),
    "wind-speed": ({"weather": True, "wind": True, "speed": True, "sensor": True, "unit": "mph"}, 6.0, 3.0, 1.2),
    "wind-direction": ({"weather": True, "wind": True, "direction": True, "sensor": True, "unit": "°"}, 220.0, 25.0, 8.0),
}


def value_at(label: str, t: float, rng: random.Random) -> float:
    """Mean + a daily cycle peaking mid-afternoon (local hour 15) + noise."""
    _tags, mean, swing, noise = POINTS[label]
    hour = time.localtime(t).tm_hour + time.localtime(t).tm_min / 60
    v = mean + swing * math.cos((hour - 15) / 24 * 2 * math.pi) + rng.uniform(-noise, noise)
    if label == "humidity":
        v = min(100.0, max(5.0, v))
    elif label == "wind-speed":
        v = max(0.0, v)
    elif label == "wind-direction":
        v %= 360
    return round(v, 1)


def publish_tags(client, topic_prefix: str, station_dis: str) -> None:
    """Equipment and point tags, retained, in the double-JSON envelope
    Baler's mqtt_listener reads."""
    equip = {"tags": {"weatherStation": True, "dis": station_dis}, "points": list(POINTS)}
    client.publish(f"{topic_prefix}/equip/tags", json.dumps({"value": json.dumps(equip)}), qos=1, retain=True)
    for label, (tags, *_rest) in POINTS.items():
        client.publish(f"{topic_prefix}/{label}/tags", json.dumps({"value": json.dumps(tags)}), qos=1, retain=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--mqtt-username", default=os.environ.get("MQTT_USERNAME"))
    p.add_argument("--mqtt-password", default=os.environ.get("MQTT_PASSWORD"))
    p.add_argument("--mqtt-tls", action="store_const", const=True, default=None,
                   help="TLS (default: on for port 8883 or MQTT_TLS=1; CA: system store or MQTT_CA_FILE)")
    p.add_argument("--topic-prefix", default=os.environ.get("WEATHER_TOPIC_PREFIX", "fbf/weather/edge"),
                   help="must be under the broker account's site prefix")
    p.add_argument("--name", default=os.environ.get("WEATHER_NAME", "Edge weather station (simulated)"))
    p.add_argument("--interval", type=float, default=60.0, help="seconds between readings")
    p.add_argument("--once", action="store_true", help="publish one round and exit")
    args = p.parse_args()

    prefix = args.topic_prefix.rstrip("/")
    client = mqtt_sink.connect(args.mqtt_host, args.mqtt_port, username=args.mqtt_username,
                               password=args.mqtt_password, tls=args.mqtt_tls)
    rng = random.Random()
    publish_tags(client, prefix, args.name)
    print(f"publishing a simulated weather station on {prefix}/* to {args.mqtt_host}:{args.mqtt_port}")
    while True:
        now = time.time()
        for label in POINTS:
            mqtt_sink.publish_reading(client, prefix, label, value_at(label, now, rng), ts=now)
        if args.once:
            time.sleep(1.0)  # let QoS 1 publishes flush
            client.loop_stop()
            client.disconnect()
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
