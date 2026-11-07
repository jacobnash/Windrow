"""
Windrow's discover -> learn -> create-connection API - the "SkySpark-easy, via
API" surface. Stdlib http.server, same reasoning as
baler/ingest_api.py: 5 routes doesn't clear the bar for a new web
framework dependency. Grew from BACnet-only to also cover Modbus discovery/
connections and a device registry (periodic scan results + credentials) -
same handler, same conventions, new routes under /modbus and /devices.

Every handler - GET included, not just writes - crosses into the asyncio
loop that owns the shared bacpypes3 Application and every manager
(ConnectionManager, ModbusConnectionManager, DeviceRegistry) via
asyncio.run_coroutine_threadsafe(), rather than letting this thread touch
that state directly: that state is loop-thread-owned, full stop, one
invariant instead of several. A timeout on every .result() call is
required, not optional - an unreachable device would otherwise wedge an
HTTP thread forever with no way to answer the client.
"""

import asyncio
import hmac
import json
import os
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow import credentials, legacy, modbus_discovery, mqtt_sink, tracing
from windrow.connection_manager import ConnectionManager
from windrow.device_registry import DeviceRegistry
from windrow.modbus_connection_manager import ModbusConnectionManager

tracer = tracing.get_tracer(__name__)

CALL_TIMEOUT = 15.0
OPENAPI_SPEC_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "openapi.yaml")


def _run(coro, loop: asyncio.AbstractEventLoop):
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=CALL_TIMEOUT)


def _redact(record: dict) -> dict:
    """Every list/get handler for a device or connection record routes
    through this - password_ciphertext never appears in an API response,
    same shape as baler/webhooks.py's `_redact_webhook`."""
    return {**record, "credential": credentials.redact(record.get("credential"))}


# The three actions multi-site.mdx's command channel dispatches by name -
# same logic the /discover, /learn, and /modbus/discover HTTP routes call,
# extracted so both callers share one implementation instead of the MQTT
# path re-deriving it.
async def handle_discover(manager: ConnectionManager, body: dict) -> list:
    return await manager.discover(**body)


async def handle_learn(manager: ConnectionManager, body: dict) -> list:
    return await manager.learn(body["device_address"], body["device_instance"])


async def handle_point_write(manager: ConnectionManager, body: dict) -> dict:
    """{topic, value (null = relinquish), priority 1-17 (17 = relinquish-default), optional expires_at}
    - BACnet WriteProperty on the point that publishes `topic`."""
    return await manager.write_point(
        body["topic"], body.get("value"), body["priority"], expires_at=body.get("expires_at")
    )


async def handle_modbus_discover(body: dict) -> list:
    return await modbus_discovery.sweep(
        body["cidr"],
        port=body.get("port", modbus_discovery.DEFAULT_PORT),
        concurrency=body.get("concurrency", modbus_discovery.DEFAULT_CONCURRENCY),
        timeout=body.get("timeout", modbus_discovery.DEFAULT_TIMEOUT),
    )


def make_handler(
    manager: ConnectionManager,
    modbus_manager: ModbusConnectionManager,
    device_registry: DeviceRegistry,
    loop: asyncio.AbstractEventLoop,
):
    gateway_secret = os.environ.get("WINDROW_GATEWAY_SECRET") or None
    cors_origin = os.environ.get("WINDROW_CORS_ORIGIN") or None

    class ConnectionsHandler(BaseHTTPRequestHandler):
        def _allowed(self) -> bool:
            """With WINDROW_GATEWAY_SECRET set (a deploy behind Baler's
            gateway), only requests the gateway forwarded - it adds
            X-Gateway-Secret after checking the caller's login and role -
            get through; anything reaching the port directly is refused.
            Unset (local dev, tests): open, as before. The spec stays public."""
            if gateway_secret is None or self.path == "/openapi.yaml":
                return True
            if hmac.compare_digest(self.headers.get("X-Gateway-Secret", ""), gateway_secret):
                return True
            self._respond_json(401, {"error": "requests must come through the gateway"})
            return False

        def _read_json_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _respond_json(self, status: int, payload) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self, span_name: str, fn):
            with tracer.start_as_current_span(span_name) as span:
                try:
                    fn(span)
                except TimeoutError as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(504, {"error": "timed out waiting on the BACnet/Modbus stack"})
                except (KeyError, ValueError, json.JSONDecodeError) as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(400, {"error": str(exc)})
                except Exception as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(500, {"error": str(exc)})

        def _credential_body(self) -> tuple[str, dict]:
            """Shared by every .../credential PATCH handler: reads
            {username, password}, encrypts it, and returns (plaintext
            password, encrypted-record-to-store) - the caller echoes the
            plaintext once in its response, same as
            baler/webhooks.py's POST /webhooks."""
            body = self._read_json_body()
            username, password = body["username"], body["password"]
            return password, credentials.encrypt(username, password)

        def do_POST(self):
            if not self._allowed():
                return
            if self.path == "/discover":
                def handle(span):
                    body = self._read_json_body()
                    devices = _run(handle_discover(manager, body), loop)
                    span.set_attribute("device_count", len(devices))
                    self._respond_json(200, devices)

                self._handle("api.post_discover", handle)
                return

            if self.path == "/write":
                def handle(span):
                    body = self._read_json_body()
                    result = _run(handle_point_write(manager, body), loop)
                    span.set_attribute("topic", body["topic"])
                    self._respond_json(200, result)

                self._handle("api.post_write", handle)
                return

            if self.path == "/learn":
                def handle(span):
                    body = self._read_json_body()
                    points = _run(handle_learn(manager, body), loop)
                    span.set_attribute("point_count", len(points))
                    self._respond_json(200, points)

                self._handle("api.post_learn", handle)
                return

            if self.path == "/connections":
                def handle(span):
                    body = self._read_json_body()
                    credential = None
                    if body.get("credential"):
                        credential = credentials.encrypt(body["credential"]["username"], body["credential"]["password"])
                    record = _run(
                        manager.create_connection(
                            device_address=body["device_address"],
                            device_instance=body["device_instance"],
                            topic_prefix=body["topic_prefix"],
                            points=body["points"],
                            poll_interval=body.get("poll_interval", 5.0),
                            credential=credential,
                        ),
                        loop,
                    )
                    span.set_attribute("connection_id", record["id"])
                    self._respond_json(201, _redact(record))

                self._handle("api.post_connections", handle)
                return

            if self.path == "/modbus/discover":
                def handle(span):
                    body = self._read_json_body()
                    devices = _run(handle_modbus_discover(body), loop)
                    span.set_attribute("device_count", len(devices))
                    self._respond_json(200, devices)

                self._handle("api.post_modbus_discover", handle)
                return

            if self.path == "/modbus/connections":
                def handle(span):
                    body = self._read_json_body()
                    credential = None
                    if body.get("credential"):
                        credential = credentials.encrypt(body["credential"]["username"], body["credential"]["password"])
                    record = _run(
                        modbus_manager.create_connection(
                            host=body["host"],
                            topic_prefix=body["topic_prefix"],
                            points=body["points"],
                            port=body.get("port", 502),
                            device_id=body.get("device_id", 1),
                            poll_interval=body.get("poll_interval", 5.0),
                            credential=credential,
                            units=body.get("units"),
                        ),
                        loop,
                    )
                    span.set_attribute("connection_id", record["id"])
                    self._respond_json(201, _redact(record))

                self._handle("api.post_modbus_connections", handle)
                return

            if self.path.startswith("/devices/") and self.path.endswith("/recheck-credentials"):
                device_id = self.path[len("/devices/") : -len("/recheck-credentials")]

                def handle(span):
                    span.set_attribute("device_id", device_id)
                    record = _run(device_registry.recheck_credentials(device_id), loop)
                    if record is None:
                        self._respond_json(404, {"error": "no device with that id"})
                        return
                    self._respond_json(200, _redact(record))

                self._handle("api.post_recheck_credentials", handle)
                return

            self.send_response(404)
            self.end_headers()

        def do_PATCH(self):
            if not self._allowed():
                return
            if self.path.startswith("/connections/") and self.path.endswith("/credential"):
                connection_id = self.path[len("/connections/") : -len("/credential")].rstrip("/")

                def handle(span):
                    span.set_attribute("connection_id", connection_id)
                    password, credential = self._credential_body()
                    record = _run(manager.set_credential(connection_id, credential), loop)
                    if record is None:
                        self._respond_json(404, {"error": "no connection with that id"})
                        return
                    self._respond_json(200, {"username": credential["username"], "password": password})

                self._handle("api.patch_connection_credential", handle)
                return

            if self.path.startswith("/modbus/connections/") and self.path.endswith("/credential"):
                connection_id = self.path[len("/modbus/connections/") : -len("/credential")].rstrip("/")

                def handle(span):
                    span.set_attribute("connection_id", connection_id)
                    password, credential = self._credential_body()
                    record = _run(modbus_manager.set_credential(connection_id, credential), loop)
                    if record is None:
                        self._respond_json(404, {"error": "no connection with that id"})
                        return
                    self._respond_json(200, {"username": credential["username"], "password": password})

                self._handle("api.patch_modbus_connection_credential", handle)
                return

            if self.path.startswith("/devices/") and self.path.endswith("/credential"):
                device_id = self.path[len("/devices/") : -len("/credential")].rstrip("/")

                def handle(span):
                    span.set_attribute("device_id", device_id)
                    password, credential = self._credential_body()
                    record = _run(device_registry.set_credential(device_id, credential), loop)
                    if record is None:
                        self._respond_json(404, {"error": "no device with that id"})
                        return
                    self._respond_json(200, {"username": credential["username"], "password": password})

                self._handle("api.patch_device_credential", handle)
                return

            if self.path.startswith("/devices/"):
                device_id = self.path[len("/devices/") :]

                def handle(span):
                    span.set_attribute("device_id", device_id)
                    body = self._read_json_body()
                    record = _run(device_registry.set_status(device_id, body["status"]), loop)
                    if record is None:
                        self._respond_json(404, {"error": "no device with that id"})
                        return
                    self._respond_json(200, _redact(record))

                self._handle("api.patch_device", handle)
                return

            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            if not self._allowed():
                return
            if self.path == "/connections":
                def handle(span):
                    connections = _run(manager.list_connections(), loop)
                    span.set_attribute("count", len(connections))
                    self._respond_json(200, [_redact(c) for c in connections])

                self._handle("api.get_connections", handle)
                return

            if self.path == "/modbus/connections":
                def handle(span):
                    connections = _run(modbus_manager.list_connections(), loop)
                    span.set_attribute("count", len(connections))
                    self._respond_json(200, [_redact(c) for c in connections])

                self._handle("api.get_modbus_connections", handle)
                return

            if self.path == "/devices":
                def handle(span):
                    devices = device_registry.list()
                    span.set_attribute("count", len(devices))
                    self._respond_json(200, [_redact(d) for d in devices])

                self._handle("api.get_devices", handle)
                return

            if self.path == "/openapi.yaml":
                with tracer.start_as_current_span("api.get_openapi_spec"):
                    with open(OPENAPI_SPEC_PATH, "rb") as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/yaml")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                return

            self.send_response(404)
            self.end_headers()

        def do_DELETE(self):
            if not self._allowed():
                return
            if self.path.startswith("/modbus/connections/"):
                connection_id = self.path[len("/modbus/connections/") :]

                def handle(span):
                    span.set_attribute("connection_id", connection_id)
                    deleted = _run(modbus_manager.delete_connection(connection_id), loop)
                    span.set_attribute("deleted", deleted)
                    self.send_response(204 if deleted else 404)
                    self.end_headers()

                self._handle("api.delete_modbus_connection", handle)
                return

            if self.path.startswith("/connections/"):
                connection_id = self.path[len("/connections/") :]

                def handle(span):
                    span.set_attribute("connection_id", connection_id)
                    deleted = _run(manager.delete_connection(connection_id), loop)
                    span.set_attribute("deleted", deleted)
                    self.send_response(204 if deleted else 404)
                    self.end_headers()

                self._handle("api.delete_connection", handle)
                return

            self.send_response(404)
            self.end_headers()

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
            self.end_headers()

        def end_headers(self):
            # Cross-origin only when asked for (WINDROW_CORS_ORIGIN, e.g.
            # http://localhost:8004 for Baler's ui_server in local dev).
            # Behind Baler's gateway the Devices page is same-origin, and a
            # wildcard here would let any website drive this API from a
            # browser that can reach it.
            if cors_origin:
                self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.send_header("Vary", "Origin")
            super().end_headers()

        def log_message(self, fmt, *args):
            pass  # quiet by default; tracing carries the real signal

    return ConnectionsHandler


def _make_on_command(manager: ConnectionManager, loop: asyncio.AbstractEventLoop, site_id: str, instance_id: str):
    """multi-site.mdx's command channel: a {"action": ..., ...} message on
    cmd/{site_id}/{instance_id}/# dispatches to the same handler its own
    HTTP route calls, and the outcome is published to .../result so a
    command isn't fire-and-forget. Runs on paho's network thread (same
    blocking-call shape as an HTTP handler's worker thread) - one command
    processes at a time, which is fine for scans you don't want overlapping.
    """
    handlers = {
        "discover": lambda body: handle_discover(manager, body),
        "learn": lambda body: handle_learn(manager, body),
        "modbus_discover": lambda body: handle_modbus_discover(body),
        "point_write": lambda body: handle_point_write(manager, body),
    }

    def on_command(client, userdata, msg):
        with tracer.start_as_current_span("api.on_command") as span:
            span.set_attribute("topic", msg.topic)
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
                handler = handlers[payload["action"]]
                # `id` is the caller's correlation id, echoed on the result
                # so concurrent callers can tell their outcomes apart.
                request_id = payload.get("id")
                body = {k: v for k, v in payload.items() if k not in ("action", "id")}
            except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as exc:
                span.set_attribute("error", str(exc))
                print(f"malformed command on {msg.topic}: {exc!r}")
                return
            try:
                outcome = {"ok": True, "result": _run(handler(body), loop)}
            except Exception as exc:  # noqa: BLE001 - one bad command must not crash the mqtt loop thread
                span.set_attribute("error", str(exc))
                outcome = {"ok": False, "error": str(exc)}
            outcome["action"] = payload["action"]
            if request_id is not None:
                outcome["id"] = request_id
            client.publish(f"cmd/{site_id}/{instance_id}/result", json.dumps(outcome), qos=1)

    return on_command


def _crash_logger(task_name: str):
    """An unhandled exception in an asyncio.Task otherwise dies silently -
    same reasoning as connection_manager.py's _make_crash_logger, applied
    to the periodic scan tasks."""

    def on_done(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        print(f"{task_name} crashed: {exc!r}")
        with tracer.start_as_current_span("api.periodic_task_crashed") as span:
            span.set_attribute("task_name", task_name)
            span.set_attribute("error", str(exc))

    return on_done


async def async_main(args) -> None:
    app = Application.from_args(args)
    await asyncio.sleep(1)  # let transport setup finish, same as bridge.py

    loop = asyncio.get_event_loop()

    # multi-site.mdx's command channel: only meaningful once this process
    # has a stable site/instance identity - a bare `windrow` run with neither
    # set behaves exactly as before (fresh session, no command topic).
    command_topic = f"cmd/{args.site_id}/{args.instance_id}/#" if args.site_id else None

    def _resubscribe_commands(client, userdata, flags, reason_code, properties):
        client.subscribe(command_topic, qos=1)

    mqtt_client = mqtt_sink.connect(
        args.mqtt_host,
        args.mqtt_port,
        client_id=f"windrow-{args.site_id}-{args.instance_id}" if args.site_id else None,
        clean_session=False if args.site_id else None,
        username=args.mqtt_username,
        password=args.mqtt_password,
        on_connect_extra=_resubscribe_commands if command_topic else None,
        tls=args.mqtt_tls,
    )
    manager = ConnectionManager(
        app, mqtt_client, args.state_file,
        site_id=args.site_id, instance_id=args.instance_id if args.site_id else None,
    )
    await manager.start()

    modbus_manager = ModbusConnectionManager(mqtt_client, args.modbus_state_file)
    await modbus_manager.start()

    if command_topic:
        mqtt_client.message_callback_add(command_topic, _make_on_command(manager, loop, args.site_id, args.instance_id))

    device_registry = DeviceRegistry(args.devices_state_file)
    await device_registry.start()

    bacnet_scan_task = loop.create_task(
        device_registry.run_periodic_bacnet_scan(app, manager, args.bacnet_discovery_interval)
    )
    bacnet_scan_task.add_done_callback(_crash_logger("bacnet periodic scan"))

    for cidr in args.modbus_scan_cidr or []:
        modbus_scan_task = loop.create_task(
            device_registry.run_periodic_modbus_scan(cidr, modbus_manager, args.modbus_scan_interval)
        )
        modbus_scan_task.add_done_callback(_crash_logger(f"modbus periodic scan ({cidr})"))

    server = ThreadingHTTPServer(
        (args.host, args.port), make_handler(manager, modbus_manager, device_registry, loop)
    )
    server.daemon_threads = True  # unset default is False - a hung request would otherwise block shutdown

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    print(f"BACnet/Modbus discovery/connections API listening on {args.host}:{args.port}")

    try:
        await asyncio.Event().wait()
    finally:
        server.shutdown()
        # modbus_manager.close() first: an unclosed AsyncModbusTcpClient
        # leaves its own internal reconnect task alive, which stalls
        # asyncio.run()'s default task-cancellation-on-shutdown
        # indefinitely - confirmed live (see test_modbus_connection_manager.py).
        await modbus_manager.close()
        mqtt_client.loop_stop()
        app.close()


def main() -> None:
    parser = SimpleArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument(
        "--mqtt-tls", action="store_const", const=True, default=None,
        help="MQTT over TLS (default: on for port 8883, or when MQTT_TLS=1; CA: system store or MQTT_CA_FILE)",
    )
    parser.add_argument("--mqtt-username", default=os.environ.get("MQTT_USERNAME"))
    parser.add_argument("--mqtt-password", default=os.environ.get("MQTT_PASSWORD"))
    parser.add_argument(
        "--site-id",
        default=legacy.env("WINDROW_SITE_ID"),
        help="Enables multi-site.mdx's command channel (cmd/<site-id>/<instance-id>/#) and a "
        "persistent MQTT session. Unset (default) behaves exactly as before - no command topic.",
    )
    parser.add_argument("--instance-id", default=legacy.env("WINDROW_INSTANCE_ID", socket.gethostname()))
    parser.add_argument("--state-file", default="windrow-connections.json")
    parser.add_argument("--modbus-state-file", default="windrow-modbus-connections.json")
    parser.add_argument("--devices-state-file", default="windrow-devices.json")
    parser.add_argument("--bacnet-discovery-interval", type=float, default=300.0)
    parser.add_argument(
        "--modbus-scan-cidr",
        action="append",
        default=None,
        help="A subnet to periodically sweep for Modbus devices, e.g. 192.168.1.0/24. Repeatable. "
        "No default - periodic Modbus scanning only runs against subnets an operator explicitly configures.",
    )
    parser.add_argument("--modbus-scan-interval", type=float, default=600.0)
    args = parser.parse_args()
    for attr in ("state_file", "modbus_state_file", "devices_state_file"):
        legacy.adopt_state_file(getattr(args, attr))

    tracing.init_tracing("windrow-api")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
