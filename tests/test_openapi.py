"""
Two layers, both required for "the docs are actually true": (1) the spec
itself is well-formed OpenAPI, (2) real live requests against the real
running API actually produce responses matching what the spec documents.
Same reasoning and same jsonschema/referencing pattern as
baler/tests/test_openapi.py.
"""

import asyncio
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import jsonschema
import mqtt_test_util
import pytest
import yaml
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from openapi_spec_validator import validate
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from windrow.api import OPENAPI_SPEC_PATH, make_handler
from windrow.connection_manager import ConnectionManager
from windrow.device_registry import DeviceRegistry
from windrow.modbus_connection_manager import ModbusConnectionManager

DEVICE_ADDRESS = "192.168.128.63:47808"
DEVICE_INSTANCE = 3456


@pytest.fixture(scope="module")
def spec() -> dict:
    with open(OPENAPI_SPEC_PATH) as f:
        return yaml.safe_load(f)


def test_spec_is_well_formed_openapi(spec):
    validate(spec)  # raises on any real structural problem


def _assert_matches_schema(instance, schema: dict, spec: dict) -> None:
    resource = Resource.from_contents(spec, default_specification=DRAFT202012)
    registry = Registry().with_resource(uri="spec", resource=resource)
    schema = _resolve_refs(schema)
    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls(schema, registry=registry).validate(instance)


def _resolve_refs(node):
    """Bare '#/...' refs only resolve against the doc they're read from -
    rewrite them to point at the registered 'spec' resource before handing
    off to jsonschema."""
    if isinstance(node, dict):
        if "$ref" in node and node["$ref"].startswith("#"):
            return {**node, "$ref": f"spec{node['$ref']}"}
        return {k: _resolve_refs(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_refs(v) for v in node]
    return node


def _schema_for(spec: dict, path: str, method: str, status: str) -> dict:
    response = spec["paths"][path][method]["responses"][status]
    if "$ref" in response:
        response = _lookup(spec, response["$ref"])
    return response["content"]["application/json"]["schema"]


def _lookup(spec: dict, ref: str) -> dict:
    node = spec
    for part in ref.lstrip("#/").split("/"):
        node = node[part]
    return node


def _make_app(bind_port: int) -> Application:
    parser = SimpleArgumentParser()
    args = parser.parse_args(["--address", f"192.168.128.63/24:{bind_port}"])
    return Application.from_args(args)


async def _request(method: str, url: str, body: dict | None = None, parse_json: bool = True):
    def do_request():
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                return resp.status, dict(resp.headers), (json.loads(raw) if (raw and parse_json) else raw)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, dict(exc.headers), (json.loads(raw) if (raw and parse_json) else raw)

    return await asyncio.to_thread(do_request)


@pytest.mark.integration
def test_full_flow_matches_documented_schemas(tmp_path, spec):
    topic_prefix = f"fbf/openapi-test-{int(time.time())}"

    async def run():
        app = _make_app(47822)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))
        await manager.start()
        modbus_manager = ModbusConnectionManager(mqtt_client, str(tmp_path / "modbus-connections.json"))
        await modbus_manager.start()
        device_registry = DeviceRegistry(str(tmp_path / "devices.json"))
        await device_registry.start()

        loop = asyncio.get_event_loop()
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(manager, modbus_manager, device_registry, loop))
        server.daemon_threads = True
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        base = f"http://127.0.0.1:{port}"

        try:
            await asyncio.sleep(1)

            # GET /openapi.yaml serves the real file
            status, headers, _ = await _request("GET", f"{base}/openapi.yaml", parse_json=False)
            assert status == 200
            assert headers["Content-Type"] == "application/yaml"

            # POST /discover
            status, _, devices = await _request("POST", f"{base}/discover", {"address": DEVICE_ADDRESS, "timeout": 5.0})
            assert status == 200
            for device in devices:
                _assert_matches_schema(device, _schema_for(spec, "/discover", "post", "200")["items"], spec)

            # POST /learn
            status, _, points = await _request(
                "POST", f"{base}/learn", {"device_address": DEVICE_ADDRESS, "device_instance": DEVICE_INSTANCE}
            )
            assert status == 200
            for point in points:
                _assert_matches_schema(point, _schema_for(spec, "/learn", "post", "200")["items"], spec)

            # POST /connections
            status, _, record = await _request(
                "POST",
                f"{base}/connections",
                {
                    "device_address": DEVICE_ADDRESS,
                    "device_instance": DEVICE_INSTANCE,
                    "topic_prefix": topic_prefix,
                    "poll_interval": 1.0,
                    "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
                },
            )
            assert status == 201
            _assert_matches_schema(record, _schema_for(spec, "/connections", "post", "201"), spec)
            connection_id = record["id"]

            # GET /connections
            status, _, listed = await _request("GET", f"{base}/connections")
            assert status == 200
            for conn in listed:
                _assert_matches_schema(conn, _schema_for(spec, "/connections", "get", "200")["items"], spec)

            # POST /discover with a bad body -> documented 400
            status, _, error_body = await _request("POST", f"{base}/learn", {"device_address": "x"})  # missing device_instance
            assert status == 400
            _assert_matches_schema(error_body, _schema_for(spec, "/learn", "post", "400"), spec)

            # DELETE /connections/{id}
            status, _, _ = await _request("DELETE", f"{base}/connections/{connection_id}")
            assert status == 204

            # DELETE again -> documented 404
            status, _, _ = await _request("DELETE", f"{base}/connections/{connection_id}")
            assert status == 404
        finally:
            server.shutdown()
            await modbus_manager.close()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())
