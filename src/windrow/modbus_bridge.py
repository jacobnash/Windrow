"""
Windrow Modbus bridge: reads holding registers on an interval, publishes to
MQTT via the same shared envelope every bridge uses (mqtt_sink). Same
skeleton as bridge.py (BACnet) - proof the pattern generalizes cheaply.
"""

import argparse
import asyncio

import pymodbus.client as ModbusClient

from windrow import mqtt_sink

POLL_INTERVAL = 5.0


def normalize_value(registers):
    """Modbus holding registers are already plain JSON-safe ints - unlike
    BACnet's Enumerated type, there's no silent-serialization trap here.
    Kept as its own function anyway so every bridge has one, on principle."""
    if not registers:
        return None
    return registers[0]


async def poll_forever(client, points: dict[str, int], mqtt_client, topic_prefix: str) -> None:
    while True:
        for label, address in points.items():
            try:
                rr = await client.read_holding_registers(address, count=1, device_id=1)
                value = None if rr.isError() else normalize_value(rr.registers)
            except Exception as exc:
                value = None
                print(f"read failed for {label}@{address}: {exc!r}")

            topic = mqtt_sink.publish_reading(mqtt_client, topic_prefix, label, value)
            print(f"published {topic} -> {value}")

        await asyncio.sleep(POLL_INTERVAL)


def parse_points(raw: list[str]) -> dict[str, int]:
    """--points zone-temp:0 fan-status:1 -> {"zone-temp": 0, "fan-status": 1}"""
    points = {}
    for item in raw:
        label, _, address = item.partition(":")
        points[label] = int(address)
    return points


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--device-host", required=True)
    parser.add_argument("--device-port", type=int, default=502)
    parser.add_argument("--points", required=True, nargs="+", help="label:address pairs, e.g. zone-temp:0 fan-status:1")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--topic-prefix", default="fbf/mock-ahu-1")
    args = parser.parse_args()

    client = ModbusClient.AsyncModbusTcpClient(args.device_host, port=args.device_port)
    await client.connect()

    mqtt_client = mqtt_sink.connect(args.mqtt_host, args.mqtt_port)

    try:
        await poll_forever(client, parse_points(args.points), mqtt_client, args.topic_prefix)
    finally:
        mqtt_client.loop_stop()
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
