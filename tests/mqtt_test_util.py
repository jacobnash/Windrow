"""Every integration test here talks to a real broker at localhot:1883 -
now mosquitto requires auth (see baler's mosquitto/acl), so every
call site needs the same MQTT_USERNAME/MQTT_PASSWORD credentials instead
of patching each test file separately."""

import os

import paho.mqtt.client as mqtt

from windrow import mqtt_sink


def connect(host: str = "localhost", port: int = 1883, **kwargs) -> mqtt.Client:
    kwargs.setdefault("username", os.environ.get("MQTT_USERNAME"))
    kwargs.setdefault("password", os.environ.get("MQTT_PASSWORD"))
    return mqtt_sink.connect(host, port, **kwargs)


def subscriber_client() -> mqtt.Client:
    """A bare (non-mqtt_sink) client for tests that subscribe directly."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    username = os.environ.get("MQTT_USERNAME")
    if username:
        client.username_pw_set(username, os.environ.get("MQTT_PASSWORD", ""))
    return client
