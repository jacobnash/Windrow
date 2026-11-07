"""Managing devices and connections remotely - the new command-channel
actions and the HTTP routes they share code with (PATCH /connections/{id},
PATCH /modbus/connections/{id}, POST /devices). Real managers and handler,
no broker or device: BACnet reads, the Modbus client and the mqtt client
are faked, and the HTTP server is bound to an ephemeral localhost port."""

import asyncio
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet

from windrow import connection_manager as cm
from windrow import default_credential_check
from windrow import modbus_connection_manager as mcm
from windrow.api import _make_on_command, make_handler
from windrow.device_registry import DeviceRegistry


class FakeMqttClient:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))


class FakeModbusClient:
    def __init__(self, record):
        self.connected = False

    async def connect(self):
        return False  # never reachable: these tests are about the records

    def close(self):
        pass


@pytest.fixture
def harness(tmp_path, monkeypatch):
    monkeypatch.setenv("WINDROW_CREDENTIAL_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(default_credential_check, "check", lambda host, endpoints=None: None)

    async def fake_read(app, address, objid):
        return 1.0

    monkeypatch.setattr(cm, "read_present_value", fake_read)

    async def fake_read_many(app, address, objids):  # the batched (RPM) path
        return {objid: await fake_read(app, address, objid) for objid in objids}

    monkeypatch.setattr(cm, "read_present_values", fake_read_many)
    monkeypatch.setattr(mcm, "RECONNECT_MAX", 60.0)

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    mqtt = FakeMqttClient()
    manager = cm.ConnectionManager(app=None, mqtt_client=mqtt, state_file=str(tmp_path / "c.json"))
    modbus_manager = mcm.ModbusConnectionManager(mqtt_client=mqtt, state_file=str(tmp_path / "m.json"))
    monkeypatch.setattr(modbus_manager, "_new_client", FakeModbusClient)
    registry = DeviceRegistry(str(tmp_path / "d.json"))
    on_command = _make_on_command(manager, loop, "site-a", "inst-1", modbus_manager, registry)

    def send(payload: dict) -> dict:
        client = FakeMqttClient()
        on_command(client, None, SimpleNamespace(topic="cmd/site-a/inst-1/x", payload=json.dumps(payload).encode()))
        assert len(client.published) == 1
        topic, raw, qos, _ = client.published[0]
        assert topic == "cmd/site-a/inst-1/result" and qos == 1
        return json.loads(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(manager, modbus_manager, registry, loop))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    def http(method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw) if raw else None

    yield SimpleNamespace(send=send, http=http, manager=manager, modbus_manager=modbus_manager, registry=registry, mqtt=mqtt)

    server.shutdown()

    async def shutdown():
        for task in manager.tasks.values():
            task.cancel()
        await modbus_manager.close()

    asyncio.run_coroutine_threadsafe(shutdown(), loop).result(timeout=5)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=2)


BACNET_CONN = {
    "device_address": "10.0.0.5", "device_instance": 5, "topic_prefix": "fbf/ahu-1", "poll_interval": 30,
    "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
    "credential": {"username": "admin", "password": "s3cret"},
}
MODBUS_CONN = {"host": "10.0.0.6", "topic_prefix": "fbf/m1", "points": {"a": 0}, "poll_interval": 30,
               "credential": {"username": "op", "password": "pw"}}


def _assert_redacted(record: dict, username: str) -> None:
    assert record["credential"] == {"username": username, "password": None}
    assert "s3cret" not in json.dumps(record) and "password_ciphertext" not in json.dumps(record)


# --- command channel ---


def test_bacnet_connection_lifecycle_over_the_command_channel(harness):
    created = harness.send({"action": "create_connection", **BACNET_CONN, "id": "req-1"})
    assert created["ok"] and created["id"] == "req-1" and created["action"] == "create_connection"
    _assert_redacted(created["result"], "admin")
    connection_id = created["result"]["id"]

    listed = harness.send({"action": "list_connections"})
    assert [c["id"] for c in listed["result"]] == [connection_id]
    _assert_redacted(listed["result"][0], "admin")

    updated = harness.send({"action": "update_connection", "id": connection_id, "poll_interval": 12})
    assert updated["ok"] and updated["id"] == connection_id
    assert updated["result"]["poll_interval"] == 12
    _assert_redacted(updated["result"], "admin")

    bad = harness.send({"action": "update_connection", "id": connection_id, "poll_interval": "fast"})
    assert bad["ok"] is False and "poll_interval" in bad["error"]

    deleted = harness.send({"action": "delete_connection", "id": connection_id})
    assert deleted == {"ok": True, "action": "delete_connection", "id": connection_id,
                       "result": {"id": connection_id, "deleted": True}}
    missing = harness.send({"action": "delete_connection", "id": connection_id})
    assert missing == {"ok": False, "action": "delete_connection", "id": connection_id,
                       "error": "no connection with that id"}


def test_modbus_connection_lifecycle_over_the_command_channel(harness):
    created = harness.send({"action": "modbus_create_connection", **MODBUS_CONN})
    assert created["ok"]
    _assert_redacted(created["result"], "op")
    connection_id = created["result"]["id"]

    updated = harness.send({"action": "modbus_update_connection", "id": connection_id, "port": 1502, "points": {"b": 4}})
    assert updated["ok"] and updated["result"]["port"] == 1502 and updated["result"]["points"] == {"b": 4}
    assert [c["port"] for c in harness.send({"action": "modbus_list_connections"})["result"]] == [1502]

    assert harness.send({"action": "modbus_update_connection", "id": "nope", "port": 1})["error"] == "no connection with that id"
    assert harness.send({"action": "modbus_update_connection", "port": 1})["error"] == "id is required"
    assert harness.send({"action": "modbus_delete_connection", "id": connection_id})["ok"]
    assert harness.send({"action": "modbus_list_connections"})["result"] == []


def test_device_actions_over_the_command_channel(harness):
    added = harness.send({"action": "add_device", "protocol": "bacnet", "device_address": "10.0.0.5", "device_instance": 5})
    assert added["ok"] and added["result"]["status"] == "pending"
    device_id = added["result"]["id"]

    assert harness.send({"action": "add_device", "protocol": "bacnet"})["ok"] is False

    status = harness.send({"action": "set_device_status", "id": device_id, "status": "ignored"})
    assert status["ok"] and status["id"] == device_id and status["result"]["status"] == "ignored"
    assert harness.send({"action": "set_device_status", "id": device_id, "status": "gone"})["ok"] is False
    assert harness.send({"action": "set_device_status", "id": "nope", "status": "ignored"})["error"] == "no device with that id"

    recheck = harness.send({"action": "recheck_credentials", "id": device_id})
    assert recheck["ok"] and recheck["result"]["id"] == device_id  # BACnet: a no-op that returns the record

    listed = harness.send({"action": "list_devices"})
    assert [d["id"] for d in listed["result"]] == [device_id]


def test_set_credential_over_the_command_channel_never_echoes_the_password(harness):
    device = harness.send({"action": "add_device", "protocol": "modbus", "host": "10.0.0.6"})["result"]
    connection = harness.send({"action": "create_connection", **BACNET_CONN, "credential": None})["result"]
    modbus = harness.send({"action": "modbus_create_connection", **MODBUS_CONN, "credential": None})["result"]

    for target, target_id in (("device", device["id"]), ("connection", connection["id"]), ("modbus_connection", modbus["id"])):
        outcome = harness.send({"action": "set_credential", "target": target, "id": target_id,
                                "credential": {"username": "admin", "password": "s3cret"}})
        assert outcome["ok"], outcome
        assert outcome["id"] == target_id
        _assert_redacted(outcome["result"], "admin")

    assert harness.registry.get(device["id"])["credential"]["password_ciphertext"]
    bad_target = harness.send({"action": "set_credential", "target": "site", "id": "x",
                               "credential": {"username": "a", "password": "b"}})
    assert bad_target["ok"] is False and "target must be one of" in bad_target["error"]
    no_password = harness.send({"action": "set_credential", "target": "device", "id": device["id"],
                                "credential": {"username": "a"}})
    assert no_password["ok"] is False and "password" in no_password["error"]


# --- HTTP routes ---


def test_patch_connection_route(harness):
    status, record = harness.http("POST", "/connections", BACNET_CONN)
    assert status == 201
    connection_id = record["id"]

    status, updated = harness.http("PATCH", f"/connections/{connection_id}", {"topic_prefix": "fbf/ahu-9"})
    assert status == 200 and updated["topic_prefix"] == "fbf/ahu-9"
    _assert_redacted(updated, "admin")

    assert harness.http("PATCH", f"/connections/{connection_id}", {"poll_interval": -1})[0] == 400
    assert harness.http("PATCH", f"/connections/{connection_id}", ["not", "an", "object"])[0] == 400
    assert harness.http("PATCH", "/connections/nope", {"poll_interval": 1}) == (404, {"error": "no connection with that id"})

    # the credential sub-route still wins over the bare /{id} route
    status, body = harness.http("PATCH", f"/connections/{connection_id}/credential", {"username": "u", "password": "p"})
    assert (status, body) == (200, {"username": "u", "password": "p"})
    assert harness.manager.connections[connection_id]["credential"]["username"] == "u"


def test_patch_modbus_connection_route(harness):
    status, record = harness.http("POST", "/modbus/connections", MODBUS_CONN)
    assert status == 201
    connection_id = record["id"]

    status, updated = harness.http("PATCH", f"/modbus/connections/{connection_id}", {"host": "10.0.0.8", "device_id": 2})
    assert status == 200 and updated["host"] == "10.0.0.8" and updated["device_id"] == 2
    assert harness.http("PATCH", f"/modbus/connections/{connection_id}", {"port": "502"})[0] == 400
    assert harness.http("PATCH", "/modbus/connections/nope", {"port": 502})[0] == 404

    status, body = harness.http("PATCH", f"/modbus/connections/{connection_id}/credential", {"username": "u", "password": "p"})
    assert (status, body) == (200, {"username": "u", "password": "p"})


def test_post_devices_route(harness):
    body = {"protocol": "modbus", "host": "10.0.0.6", "port": 502, "device_id": 1}
    status, record = harness.http("POST", "/devices", body)
    assert status == 201 and record["status"] == "pending" and record["protocol"] == "modbus"
    status, again = harness.http("POST", "/devices", body)
    assert status == 200 and again["id"] == record["id"]

    status, error = harness.http("POST", "/devices", {"protocol": "bacnet", "device_address": "10.0.0.5"})
    assert status == 400 and "device_instance" in error["error"]
    assert harness.http("POST", "/devices", {"protocol": "zigbee"})[0] == 400

    status, devices = harness.http("GET", "/devices")
    assert status == 200 and [d["id"] for d in devices] == [record["id"]]

    assert harness.http("PATCH", f"/devices/{record['id']}", {"status": "nonsense"})[0] == 400
    assert harness.http("PATCH", f"/devices/{record['id']}", {"status": "ignored"})[1]["status"] == "ignored"


def test_delete_routes_still_answer_204_then_404(harness):
    _, record = harness.http("POST", "/connections", BACNET_CONN)
    assert harness.http("DELETE", f"/connections/{record['id']}") == (204, None)
    assert harness.http("DELETE", f"/connections/{record['id']}") == (404, None)
    _, record = harness.http("POST", "/modbus/connections", MODBUS_CONN)
    assert harness.http("DELETE", f"/modbus/connections/{record['id']}") == (204, None)
    assert harness.http("DELETE", f"/modbus/connections/{record['id']}") == (404, None)


def test_new_routes_match_the_documented_schemas(harness):
    import yaml
    from test_openapi import _assert_matches_schema, _schema_for

    from windrow.api import OPENAPI_SPEC_PATH

    with open(OPENAPI_SPEC_PATH) as f:
        spec = yaml.safe_load(f)

    # Without credentials: Connection's allOf inherits CreateConnectionRequest's
    # write-side `credential` schema, which a redacted credential doesn't match
    # (a pre-existing spec gap, separate from these routes).
    _, record = harness.http("POST", "/connections", {**BACNET_CONN, "credential": None})
    status, updated = harness.http("PATCH", f"/connections/{record['id']}", {"poll_interval": 9})
    _assert_matches_schema(updated, _schema_for(spec, "/connections/{id}", "patch", str(status)), spec)

    _, record = harness.http("POST", "/modbus/connections", {**MODBUS_CONN, "credential": None})
    status, updated = harness.http("PATCH", f"/modbus/connections/{record['id']}", {"port": 1502})
    _assert_matches_schema(updated, _schema_for(spec, "/modbus/connections/{id}", "patch", str(status)), spec)

    for body in ({"protocol": "bacnet", "device_address": "10.0.0.5", "device_instance": 1234},
                 {"protocol": "modbus", "host": "10.0.0.6", "port": 502, "device_id": 1}):
        _assert_matches_schema(body, spec["components"]["schemas"]["AddDeviceRequest"], spec)
        status, device = harness.http("POST", "/devices", body)
        _assert_matches_schema(device, _schema_for(spec, "/devices", "post", str(status)), spec)

    status, error = harness.http("PATCH", "/connections/nope", {"poll_interval": 1})
    _assert_matches_schema(error, _schema_for(spec, "/connections/{id}", "patch", str(status)), spec)
