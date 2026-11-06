"""
Windrow bridge: reads configured BACnet points on an interval, publishes
readings to MQTT as JSON. Haxall's existing open hxMqtt connector
subscribes on the other end — no Fantom code involved.
"""

import asyncio

from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import mqtt_sink
from windrow.bacnet_values import normalize_value

POLL_INTERVAL = 5.0


async def poll_forever(app: Application, device_address: str, points: list[str], mqtt_client, topic_prefix: str) -> None:
    while True:
        for point in points:
            try:
                value = normalize_value(await app.read_property(device_address, point, "present-value"))
            except Exception as exc:
                value = None
                print(f"read failed for {point}: {exc!r}")

            topic = mqtt_sink.publish_reading(mqtt_client, topic_prefix, point, value)
            print(f"published {topic} -> {value}")

        await asyncio.sleep(POLL_INTERVAL)


async def main() -> None:
    parser = SimpleArgumentParser()
    parser.add_argument("--device-address", required=True, help="BACnet device address, e.g. 192.168.128.63:47808")
    parser.add_argument("--points", required=True, nargs="+", help="Object identifiers to poll, e.g. analogValue,1 binaryValue,1")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--topic-prefix", default="fbf/mock-ahu-1")
    args = parser.parse_args()

    app = Application.from_args(args)
    await asyncio.sleep(1)  # let transport setup finish

    mqtt_client = mqtt_sink.connect(args.mqtt_host, args.mqtt_port)

    try:
        await poll_forever(app, args.device_address, args.points, mqtt_client, args.topic_prefix)
    finally:
        mqtt_client.loop_stop()
        app.close()


if __name__ == "__main__":
    asyncio.run(main())
