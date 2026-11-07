"""The command channel's retained status heartbeat on cmd/{site}/{instance}/status,
its offline last will, and the clean-shutdown offline message - no broker
needed (fake mqtt client / patched paho Client)."""

import asyncio
import datetime
import json
from types import SimpleNamespace

from windrow import api, mqtt_sink


class FakeMqttClient:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return SimpleNamespace(wait_for_publish=lambda timeout=None: None)


def _managers():
    manager = SimpleNamespace(connections={"a": {}, "b": {}},
                              read_stats=lambda: {"read_property_multiple": 4, "cov_subscriptions": 1})
    modbus_manager = SimpleNamespace(connections={"m": {}})
    registry = SimpleNamespace(list=lambda: [{"status": "pending"}, {"status": "provisioned"}, {"status": "pending"}])
    return manager, modbus_manager, registry


def test_build_status_shape():
    status = api.build_status("site-a", "inst-1", "2026-01-01T00:00:00+00:00", *_managers())
    assert set(status) == {
        "online", "site_id", "instance_id", "version", "started_at", "ts",
        "bacnet_connections", "bacnet_network", "bacnet_requests", "modbus_connections", "devices", "pending_devices",
    }
    assert status["online"] is True
    assert (status["site_id"], status["instance_id"]) == ("site-a", "inst-1")
    assert status["version"] == api.package_version() and status["version"]
    assert status["started_at"] == "2026-01-01T00:00:00+00:00"
    assert datetime.datetime.fromisoformat(status["ts"]).tzinfo is not None
    assert (status["bacnet_connections"], status["modbus_connections"]) == (2, 1)
    assert (status["devices"], status["pending_devices"]) == (3, 2)
    assert status["bacnet_network"] == {"mode": "normal"}  # no app in this fake: a plain local link
    assert status["bacnet_requests"] == {"read_property_multiple": 4, "cov_subscriptions": 1}
    json.dumps(status)


def test_package_version_falls_back_to_dev(monkeypatch):
    def missing(name):
        raise api.importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(api.importlib.metadata, "version", missing)
    assert api.package_version() == "dev"


def test_heartbeat_publishes_retained_at_start_then_every_interval(monkeypatch):
    monkeypatch.setattr(api, "STATUS_INTERVAL", 0.05)
    client = FakeMqttClient()
    calls = []

    def build():
        calls.append(1)
        return {"online": True, "n": len(calls)}

    async def run():
        task = asyncio.create_task(api.run_status_heartbeat(client, "cmd/site-a/inst-1/status", build))
        await asyncio.sleep(0.01)
        assert len(client.published) == 1  # straight away, not after the first interval
        await asyncio.sleep(0.12)
        task.cancel()

    asyncio.run(run())
    assert len(client.published) >= 3
    for topic, payload, qos, retain in client.published:
        assert topic == "cmd/site-a/inst-1/status" and qos == 1 and retain is True
    assert [json.loads(p)["n"] for _, p, _, _ in client.published] == list(range(1, len(client.published) + 1))


def test_heartbeat_wake_republishes_immediately(monkeypatch):
    """A reconnect sets `wake` so "online" replaces the broker's will at once."""
    monkeypatch.setattr(api, "STATUS_INTERVAL", 60.0)
    client = FakeMqttClient()

    async def run():
        wake = asyncio.Event()
        task = asyncio.create_task(api.run_status_heartbeat(client, "t", lambda: {"online": True}, wake))
        await asyncio.sleep(0.01)
        wake.set()
        await asyncio.sleep(0.01)
        task.cancel()

    asyncio.run(run())
    assert len(client.published) == 2


def test_heartbeat_survives_a_failing_publish(monkeypatch, capsys):
    monkeypatch.setattr(api, "STATUS_INTERVAL", 0.01)
    attempts = []

    class Flaky:
        def publish(self, *args, **kwargs):
            attempts.append(1)
            raise RuntimeError("boom")

    async def run():
        task = asyncio.create_task(api.run_status_heartbeat(Flaky(), "t", dict))
        await asyncio.sleep(0.05)
        task.cancel()

    asyncio.run(run())
    assert len(attempts) >= 2
    assert "status heartbeat failed" in capsys.readouterr().out


def test_publish_offline_is_retained_and_waited_for():
    client = FakeMqttClient()
    api.publish_offline(client, "site-a", "inst-1")
    assert client.published == [(
        "cmd/site-a/inst-1/status",
        json.dumps({"online": False, "site_id": "site-a", "instance_id": "inst-1"}),
        1,
        True,
    )]


def test_publish_offline_never_raises_on_shutdown():
    class Broken:
        def publish(self, *args, **kwargs):
            raise RuntimeError("not connected")

    api.publish_offline(Broken(), "site-a", "inst-1")  # must not raise


def test_connect_registers_the_last_will_before_connecting(monkeypatch):
    calls = []

    class FakePahoClient:
        def __init__(self, *args, **kwargs):
            pass

        def will_set(self, topic, payload, qos=0, retain=False):
            calls.append(("will_set", topic, payload, qos, retain))

        def connect(self, host, port):
            calls.append(("connect", host, port))

        def loop_start(self):
            calls.append(("loop_start",))

    monkeypatch.setattr(mqtt_sink.mqtt, "Client", FakePahoClient)
    will = (api.status_topic("site-a", "inst-1"), api.offline_payload("site-a", "inst-1"))
    mqtt_sink.connect("broker", 1883, tls=False, will=will)
    assert calls[0] == ("will_set", "cmd/site-a/inst-1/status", will[1], 1, True)
    assert calls[1] == ("connect", "broker", 1883)


def test_connect_without_a_will_sets_none(monkeypatch):
    calls = []

    class FakePahoClient:
        def __init__(self, *args, **kwargs):
            pass

        def will_set(self, *args, **kwargs):
            calls.append("will_set")

        def connect(self, host, port):
            pass

        def loop_start(self):
            pass

    monkeypatch.setattr(mqtt_sink.mqtt, "Client", FakePahoClient)
    mqtt_sink.connect("broker", 1883, tls=False)
    assert calls == []


def test_clear_retained_publishes_an_empty_retained_payload():
    client = FakeMqttClient()
    assert mqtt_sink.clear_retained(client, "fbf/ahu-1", "zone-temp/tags") == "fbf/ahu-1/zone-temp/tags"
    assert client.published == [("fbf/ahu-1/zone-temp/tags", b"", 1, True)]
