"""Unit tests for _make_on_command (multi-site.mdx's command channel) -
no real MQTT broker or BACnet device needed, since the dispatch logic
itself doesn't touch either: it's given a fake mqtt client to publish the
result to and a fake manager standing in for ConnectionManager."""

import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from windrow.api import _make_on_command


class FakeManager:
    async def discover(self, **kwargs):
        return [{"discovered": kwargs}]

    async def learn(self, device_address, device_instance):
        return [{"device_address": device_address, "device_instance": device_instance}]


class FakeMqttClient:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, qos=0):
        self.published.append((topic, payload, qos))


@pytest.fixture
def loop_thread():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=2)


def _send(loop, action_body: dict) -> FakeMqttClient:
    on_command = _make_on_command(FakeManager(), loop, "site-a", "inst-1")
    client = FakeMqttClient()
    msg = SimpleNamespace(topic="cmd/site-a/inst-1", payload=json.dumps(action_body).encode("utf-8"))
    on_command(client, None, msg)
    return client


def test_dispatches_discover_and_publishes_result(loop_thread):
    client = _send(loop_thread, {"action": "discover", "low_limit": 0, "high_limit": 10})
    assert len(client.published) == 1
    topic, payload, qos = client.published[0]
    assert topic == "cmd/site-a/inst-1/result"
    assert qos == 1
    outcome = json.loads(payload)
    assert outcome == {"ok": True, "result": [{"discovered": {"low_limit": 0, "high_limit": 10}}]}


def test_dispatches_learn(loop_thread):
    client = _send(loop_thread, {"action": "learn", "device_address": "10.0.0.5", "device_instance": 42})
    outcome = json.loads(client.published[0][1])
    assert outcome == {
        "ok": True,
        "result": [{"device_address": "10.0.0.5", "device_instance": 42}],
    }


def test_malformed_json_does_not_crash_or_publish(loop_thread):
    on_command = _make_on_command(FakeManager(), loop_thread, "site-a", "inst-1")
    client = FakeMqttClient()
    msg = SimpleNamespace(topic="cmd/site-a/inst-1", payload=b"not json")
    on_command(client, None, msg)  # must not raise
    assert client.published == []


def test_unknown_action_does_not_crash_or_publish(loop_thread):
    client = _send(loop_thread, {"action": "reboot_the_moon"})
    assert client.published == []


def test_handler_exception_reports_error_outcome_instead_of_crashing(loop_thread):
    on_command = _make_on_command(FakeManager(), loop_thread, "site-a", "inst-1")
    client = FakeMqttClient()
    # "learn" without the required device_address/device_instance keys -
    # a KeyError inside the handler coroutine itself, not a malformed
    # message - must still surface as an {"ok": false} outcome, not crash
    # the mqtt network thread.
    msg = SimpleNamespace(topic="cmd/site-a/inst-1", payload=json.dumps({"action": "learn"}).encode("utf-8"))
    on_command(client, None, msg)
    outcome = json.loads(client.published[0][1])
    assert outcome["ok"] is False
    assert "error" in outcome
