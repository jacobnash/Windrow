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
import contextlib
import datetime
import hmac
import importlib.metadata
import ipaddress
import json
import os
import signal
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.argparse import SimpleArgumentParser

from windrow import (
    announce,
    bacnet_alarms,
    bacnet_network,
    connection_store,
    credentials,
    legacy,
    modbus_discovery,
    mqtt_sink,
    tracing,
)
from windrow.connection_manager import READ_OPTION_FIELDS, ConnectionManager
from windrow.device_registry import DeviceRegistry
from windrow.discovery import discover_params
from windrow.jobs import JobRunner
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


def _with_poll_status(owner, record: dict) -> dict:
    """A listed connection plus `poll_status` (poll_status.py, Baler#110):
    whether it's actually reading. Added to the response only - the stored
    record never carries it."""
    out = _redact(record)
    status = getattr(owner, "status", None)
    if status is not None:
        out["poll_status"] = status.snapshot(record["id"], len(record.get("points") or ()))
    return out


class NotFound(LookupError):
    """No device/connection with the id a handler was given - a 404 over
    HTTP, an {"ok": false} result over the command channel."""


def _target_id(body: dict) -> str:
    target = body.get("id")
    if not isinstance(target, str) or not target:
        raise ValueError("id is required")
    return target


def _encrypt_credential(credential) -> dict:
    if not isinstance(credential, dict):
        raise ValueError("credential must be an object with username and password")  # noqa: TRY004 - ValueError means 400 / ok:false
    return credentials.encrypt(credential["username"], credential["password"])


# Every action the command channel dispatches by name, and the code behind
# the matching HTTP route - extracted so both callers share one
# implementation instead of the MQTT path re-deriving it. Each takes the
# request body as a dict and returns something JSON-serializable, with
# credentials already redacted the way every HTTP GET redacts them.
async def handle_discover(manager: ConnectionManager, body: dict) -> list:
    return await manager.discover(**discover_params(body))


async def handle_discover_routers(manager: ConnectionManager, body: dict) -> list:
    """Who-Is-Router-To-Network: {network?, timeout?} -> [{address, networks}]."""
    network = body.get("network")
    if network is not None and (isinstance(network, bool) or not isinstance(network, int) or not 1 <= network <= 65534):
        raise ValueError(f"network must be a BACnet network number 1-65534, got {network!r}")
    timeout = connection_store.check_number("timeout", body.get("timeout", 5.0))
    return await bacnet_network.discover_routers(manager.app, network=network, timeout=timeout)


async def handle_list_announced(announce_registry, body: dict | None = None) -> list:
    """Devices that announced themselves to this node (windrow.announce) -
    the same list GET /announced returns, for the command channel too."""
    return announce_registry.list() if announce_registry else []


async def handle_network_status(manager: ConnectionManager, body: dict | None = None) -> dict:
    return bacnet_network.network_status(manager.app)


async def handle_learn(manager: ConnectionManager, body: dict) -> list:
    return await manager.learn(body["device_address"], body["device_instance"])


async def handle_read_schedules(manager: ConnectionManager, body: dict) -> dict:
    """{device_address, device_instance} -> the device's schedules and calendars."""
    instance = connection_store.check_int("device_instance", body.get("device_instance"), 0, 4194302)
    return await manager.read_schedules(connection_store.check_text("device_address", body.get("device_address")), instance)


async def handle_connection_schedules(manager: ConnectionManager, body: dict) -> dict:
    """{id} -> the connection's device's schedules, controlled points named by label."""
    result = await manager.connection_schedules(_target_id(body))
    if result is None:
        raise NotFound("no connection with that id")
    return result


async def handle_write_schedule(manager: ConnectionManager, body: dict) -> dict:
    """{id: connection id, object: "schedule,N", weekly?, exceptions?,
    default?, effective_period?, expires_at?, who?, reason?} -> the
    schedule read back (Windrow#26). Command channel only."""
    return await _schedule_write(manager.write_schedule, body)


async def handle_write_calendar(manager: ConnectionManager, body: dict) -> dict:
    """{id, object: "calendar,N", dates, expires_at?, who?, reason?} -> the calendar read back."""
    return await _schedule_write(manager.write_calendar, body)


async def _schedule_write(write, body: dict) -> dict:
    connection_id = _target_id(body)
    fields = {k: v for k, v in body.items() if k not in ("id", "expires_at", "who", "reason")}
    who, reason = body.get("who"), body.get("reason")
    result = await write(connection_id, fields, expires_at=body.get("expires_at"),
                         who=who if isinstance(who, str) else None, reason=reason if isinstance(reason, str) else None)
    if result is None:
        raise NotFound("no connection with that id")
    return result


async def handle_point_write(
    manager: ConnectionManager, body: dict, modbus_manager: ModbusConnectionManager | None = None
) -> dict:
    """{topic, value (null = relinquish), priority 1-17 (17 = relinquish-default), optional expires_at}
    - BACnet WriteProperty on the point that publishes `topic`, or a Modbus
    register/coil write if a Modbus connection publishes it (Baler#128).
    Same command either way, so Baler needs no protocol switch."""
    owner = modbus_manager if modbus_manager is not None and modbus_manager.owns(body["topic"]) else manager
    return await owner.write_point(
        body["topic"], body.get("value"), body["priority"], expires_at=body.get("expires_at")
    )


async def handle_modbus_discover(body: dict) -> list:
    return await modbus_discovery.sweep(
        body["cidr"],
        port=body.get("port", modbus_discovery.DEFAULT_PORT),
        concurrency=body.get("concurrency", modbus_discovery.DEFAULT_CONCURRENCY),
        timeout=body.get("timeout", modbus_discovery.DEFAULT_TIMEOUT),
    )


# --- background jobs (jobs.py, Baler#139) ---


async def handle_start_job(jobs: JobRunner, body: dict) -> dict:
    """{job: discover|learn|modbus_discover, ...} - answers at once; poll get_job."""
    return await jobs.start(body)


async def handle_get_job(jobs: JobRunner, body: dict) -> dict:
    job = jobs.get(_target_id(body))
    if job is None:
        raise NotFound("no job with that id (finished jobs are kept for a while, and not across a restart)")
    return job


async def handle_list_jobs(jobs: JobRunner, body: dict | None = None) -> list:
    return jobs.list()


# --- BACnet connections ---


async def handle_list_connections(manager: ConnectionManager, body: dict | None = None) -> list:
    return [_with_poll_status(manager, c) for c in await manager.list_connections()]


async def handle_create_connection(manager: ConnectionManager, body: dict) -> dict:
    credential = _encrypt_credential(body["credential"]) if body.get("credential") else None
    record = await manager.create_connection(
        device_address=body["device_address"],
        device_instance=body["device_instance"],
        topic_prefix=body["topic_prefix"],
        points=body["points"],
        poll_interval=body.get("poll_interval", 5.0),
        credential=credential,
        device_name=body.get("device_name"),
        read_options={k: body[k] for k in READ_OPTION_FIELDS if k in body},
    )
    return _redact(record)


async def handle_update_connection(manager: ConnectionManager, body: dict) -> dict:
    """{id, ...patch} - patch is any of points/poll_interval/topic_prefix and
    the read options (read_multiple, read_multiple_max, subscribe_cov,
    cov_lifetime, cov_poll_interval, forward_alarms)."""
    connection_id = _target_id(body)
    record = await manager.update_connection(connection_id, {k: v for k, v in body.items() if k != "id"})
    if record is None:
        raise NotFound("no connection with that id")
    return _redact(record)


async def handle_connection_alarms(manager: ConnectionManager, body: dict) -> list:
    """{id} -> the device's active alarms, read now (GetEventInformation)."""
    events = await manager.active_alarms(_target_id(body))
    if events is None:
        raise NotFound("no connection with that id")
    return events


async def handle_delete_connection(manager: ConnectionManager, body: dict) -> dict:
    connection_id = _target_id(body)
    if not await manager.delete_connection(connection_id):
        raise NotFound("no connection with that id")
    return {"id": connection_id, "deleted": True}


# --- Modbus connections ---


async def handle_modbus_list_connections(modbus_manager: ModbusConnectionManager, body: dict | None = None) -> list:
    return [_with_poll_status(modbus_manager, c) for c in await modbus_manager.list_connections()]


async def handle_modbus_test_read(modbus_manager: ModbusConnectionManager, body: dict) -> dict:
    """{host, register, port?, device_id?, point?} - one register from a
    device before it's a connection, so a wrong host/unit/register shows up
    before saving (Baler#110). Nothing is stored or published."""
    return await modbus_manager.test_read(
        body["host"], body["register"], port=body.get("port", 502), device_id=body.get("device_id", 1),
        point=body.get("point"),
    )


async def handle_modbus_create_connection(modbus_manager: ModbusConnectionManager, body: dict) -> dict:
    credential = _encrypt_credential(body["credential"]) if body.get("credential") else None
    record = await modbus_manager.create_connection(
        host=body["host"],
        topic_prefix=body["topic_prefix"],
        points=body["points"],
        port=body.get("port", 502),
        device_id=body.get("device_id", 1),
        poll_interval=body.get("poll_interval", 5.0),
        credential=credential,
        units=body.get("units"),
        device_name=body.get("device_name"),
    )
    return _redact(record)


async def handle_modbus_update_connection(modbus_manager: ModbusConnectionManager, body: dict) -> dict:
    """{id, ...patch} - patch is any of points/poll_interval/topic_prefix/host/port/device_id."""
    connection_id = _target_id(body)
    record = await modbus_manager.update_connection(connection_id, {k: v for k, v in body.items() if k != "id"})
    if record is None:
        raise NotFound("no connection with that id")
    return _redact(record)


async def handle_modbus_delete_connection(modbus_manager: ModbusConnectionManager, body: dict) -> dict:
    connection_id = _target_id(body)
    if not await modbus_manager.delete_connection(connection_id):
        raise NotFound("no connection with that id")
    return {"id": connection_id, "deleted": True}


# --- devices ---


async def handle_list_devices(device_registry: DeviceRegistry, body: dict | None = None) -> list:
    return [_redact(d) for d in device_registry.list()]


async def handle_set_device_status(device_registry: DeviceRegistry, body: dict) -> dict:
    record = await device_registry.set_status(_target_id(body), body["status"])
    if record is None:
        raise NotFound("no device with that id")
    return _redact(record)


async def add_device(
    device_registry: DeviceRegistry,
    manager: ConnectionManager,
    modbus_manager: ModbusConnectionManager,
    body: dict,
) -> tuple[dict, bool]:
    """(redacted record, whether it was newly created). Passes the
    matching protocol's connections along so a device that's already
    being polled comes back provisioned, the way a scan would mark it."""
    before = set(device_registry.records)
    owner = modbus_manager if isinstance(body, dict) and body.get("protocol") == "modbus" else manager
    record = await device_registry.add_device(body, list(owner.connections.values()))
    return _redact(record), record["id"] not in before


async def handle_add_device(device_registry, manager, modbus_manager, body: dict) -> dict:
    record, _ = await add_device(device_registry, manager, modbus_manager, body)
    return record


async def handle_recheck_credentials(device_registry: DeviceRegistry, body: dict) -> dict:
    record = await device_registry.recheck_credentials(_target_id(body))
    if record is None:
        raise NotFound("no device with that id")
    return _redact(record)


CREDENTIAL_TARGETS = ("device", "connection", "modbus_connection")


async def set_credential(target: str, target_id: str, credential: dict, manager, modbus_manager, device_registry) -> dict:
    """Stores an already-encrypted credential on a device or connection;
    the raw stored record (callers redact or echo as they need)."""
    setters = {
        "device": (device_registry, "no device with that id"),
        "connection": (manager, "no connection with that id"),
        "modbus_connection": (modbus_manager, "no connection with that id"),
    }
    if target not in setters:
        raise ValueError(f"target must be one of {', '.join(CREDENTIAL_TARGETS)}, got {target!r}")
    owner, missing = setters[target]
    record = await owner.set_credential(target_id, credential)
    if record is None:
        raise NotFound(missing)
    return record


async def handle_set_credential(manager, modbus_manager, device_registry, body: dict) -> dict:
    """{target, id, credential: {username, password}}. Unlike the HTTP
    PATCH, the result doesn't echo the password back: a result topic is
    readable by anything subscribed to it, and the sender already has it."""
    record = await set_credential(
        body["target"], _target_id(body), _encrypt_credential(body.get("credential")),
        manager, modbus_manager, device_registry,
    )
    return _redact(record)


def make_handler(
    manager: ConnectionManager,
    modbus_manager: ModbusConnectionManager,
    device_registry: DeviceRegistry,
    loop: asyncio.AbstractEventLoop,
    status_fn=None,
    jobs: JobRunner | None = None,
    announce_registry: "announce.AnnounceRegistry | None" = None,
):
    """`status_fn` builds GET /status - the same payload as the MQTT
    heartbeat, for a node Baler reaches over HTTP (Baler#114). `jobs` runs
    /jobs, the slow actions in the background (Baler#139)."""
    if jobs is None:
        jobs = JobRunner(manager, modbus_manager, device_registry)
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
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(body, dict):
                raise ValueError("request body must be a JSON object")  # noqa: TRY004 - ValueError means 400 / ok:false
            return body

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
                except NotFound as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(404, {"error": str(exc)})
                except TimeoutError as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(504, {"error": "timed out waiting on the BACnet/Modbus stack"})
                except (KeyError, ValueError, json.JSONDecodeError) as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(400, {"error": str(exc)})
                except Exception as exc:
                    span.set_attribute("error", str(exc))
                    self._respond_json(500, {"error": str(exc)})
                except ErrorRejectAbortNack as exc:
                    # A device's Error/Reject/Abort that a handler let through
                    # (bacpypes3 raises them, as BaseExceptions): still an
                    # answer to the client, not a dropped connection.
                    span.set_attribute("error", str(exc))
                    self._respond_json(502, {"error": f"the device answered: {exc}"})

        def _route(self, span_name: str, handler, status: int = 200, body: dict | None = None, count: bool = False):
            """The common shape: run one handle_* coroutine on the loop and
            answer with its result as JSON. `body=None` reads the request's."""

            def handle(span):
                result = _run(handler(self._read_json_body() if body is None else body), loop)
                if count:
                    span.set_attribute("count", len(result))
                self._respond_json(status, result)

            self._handle(span_name, handle)

        def _path_id(self, prefix: str, suffix: str = "") -> str | None:
            """The {id} in prefix + {id} + suffix, or None if the path isn't
            that shape - an id never contains a slash, which is what keeps
            /connections/{id} from swallowing /connections/{id}/credential."""
            if not (self.path.startswith(prefix) and self.path.endswith(suffix)):
                return None
            target = self.path[len(prefix) : len(self.path) - len(suffix)].rstrip("/") if suffix else self.path[len(prefix) :]
            if not target or "/" in target:
                return None
            return target

        def _credential_body(self) -> tuple[str, dict]:
            """Shared by every .../credential PATCH handler: reads
            {username, password}, encrypts it, and returns (plaintext
            password, encrypted-record-to-store) - the caller echoes the
            plaintext once in its response, same as
            baler/webhooks.py's POST /webhooks."""
            body = self._read_json_body()
            username, password = body["username"], body["password"]
            return password, credentials.encrypt(username, password)

        def _patch_credential(self, span_name: str, target: str, target_id: str) -> None:
            def handle(span):
                span.set_attribute("target_id", target_id)
                password, credential = self._credential_body()
                _run(set_credential(target, target_id, credential, manager, modbus_manager, device_registry), loop)
                self._respond_json(200, {"username": credential["username"], "password": password})

            self._handle(span_name, handle)

        def do_POST(self):
            if not self._allowed():
                return
            if self.path == "/discover":
                self._route("api.post_discover", lambda body: handle_discover(manager, body), count=True)
                return

            if self.path == "/discover/routers":
                self._route("api.post_discover_routers", lambda body: handle_discover_routers(manager, body), count=True)
                return

            if self.path == "/write":
                self._route("api.post_write", lambda body: handle_point_write(manager, body, modbus_manager))
                return

            if self.path == "/learn":
                self._route("api.post_learn", lambda body: handle_learn(manager, body), count=True)
                return

            if self.path == "/schedules":
                self._route("api.post_schedules", lambda body: handle_read_schedules(manager, body))
                return

            if self.path == "/connections":
                self._route("api.post_connections", lambda body: handle_create_connection(manager, body), status=201)
                return

            if self.path == "/modbus/discover":
                self._route("api.post_modbus_discover", handle_modbus_discover, count=True)
                return

            if self.path == "/jobs":
                self._route("api.post_jobs", lambda body: handle_start_job(jobs, body), status=202)
                return

            if self.path == "/modbus/test-read":
                self._route("api.post_modbus_test_read", lambda body: handle_modbus_test_read(modbus_manager, body))
                return

            if self.path == "/modbus/connections":
                self._route(
                    "api.post_modbus_connections",
                    lambda body: handle_modbus_create_connection(modbus_manager, body),
                    status=201,
                )
                return

            if self.path == "/devices":
                def handle(span):
                    record, created = _run(add_device(device_registry, manager, modbus_manager, self._read_json_body()), loop)
                    span.set_attribute("created", created)
                    self._respond_json(201 if created else 200, record)

                self._handle("api.post_devices", handle)
                return

            device_id = self._path_id("/devices/", "/recheck-credentials")
            if device_id is not None:
                self._route(
                    "api.post_recheck_credentials",
                    lambda body: handle_recheck_credentials(device_registry, body),
                    body={"id": device_id},
                )
                return

            self.send_response(404)
            self.end_headers()

        def do_PATCH(self):
            if not self._allowed():
                return
            # .../credential first: the bare /{id} routes below would
            # otherwise be the ones to match.
            connection_id = self._path_id("/connections/", "/credential")
            if connection_id is not None:
                self._patch_credential("api.patch_connection_credential", "connection", connection_id)
                return

            connection_id = self._path_id("/modbus/connections/", "/credential")
            if connection_id is not None:
                self._patch_credential("api.patch_modbus_connection_credential", "modbus_connection", connection_id)
                return

            device_id = self._path_id("/devices/", "/credential")
            if device_id is not None:
                self._patch_credential("api.patch_device_credential", "device", device_id)
                return

            connection_id = self._path_id("/connections/")
            if connection_id is not None:
                self._route(
                    "api.patch_connection",
                    lambda body: handle_update_connection(manager, {**body, "id": connection_id}),
                )
                return

            connection_id = self._path_id("/modbus/connections/")
            if connection_id is not None:
                self._route(
                    "api.patch_modbus_connection",
                    lambda body: handle_modbus_update_connection(modbus_manager, {**body, "id": connection_id}),
                )
                return

            device_id = self._path_id("/devices/")
            if device_id is not None:
                self._route(
                    "api.patch_device",
                    lambda body: handle_set_device_status(device_registry, {**body, "id": device_id}),
                )
                return

            self.send_response(404)
            self.end_headers()

        def do_GET(self):
            if not self._allowed():
                return
            if self.path == "/connections":
                self._route("api.get_connections", lambda _: handle_list_connections(manager), body={}, count=True)
                return

            if self.path == "/modbus/connections":
                self._route(
                    "api.get_modbus_connections", lambda _: handle_modbus_list_connections(modbus_manager),
                    body={}, count=True,
                )
                return

            if self.path == "/devices":
                self._route("api.get_devices", lambda _: handle_list_devices(device_registry), body={}, count=True)
                return

            if self.path == "/announced":
                self._route("api.get_announced", lambda _: handle_list_announced(announce_registry), body={}, count=True)
                return

            if self.path == "/jobs":
                self._route("api.get_jobs", lambda _: handle_list_jobs(jobs), body={}, count=True)
                return

            connection_id = self._path_id("/connections/", "/alarms")
            if connection_id is not None:
                self._route("api.get_connection_alarms", lambda body: handle_connection_alarms(manager, body),
                            body={"id": connection_id})
                return

            connection_id = self._path_id("/connections/", "/schedules")
            if connection_id is not None:
                self._route("api.get_connection_schedules", lambda body: handle_connection_schedules(manager, body),
                            body={"id": connection_id})
                return

            job_id = self._path_id("/jobs/")
            if job_id is not None:
                self._route("api.get_job", lambda body: handle_get_job(jobs, body), body={"id": job_id})
                return

            if self.path == "/status" and status_fn is not None:
                async def status(_body):  # on the loop, like every other read (module docstring)
                    return status_fn()

                self._route("api.get_status", status, body={})
                return

            if self.path == "/bacnet/network":
                self._route("api.get_bacnet_network", lambda _: handle_network_status(manager), body={})
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

        def _delete(self, span_name: str, handler, connection_id: str) -> None:
            def handle(span):
                span.set_attribute("connection_id", connection_id)
                try:
                    _run(handler({"id": connection_id}), loop)
                except NotFound:
                    deleted = False
                else:
                    deleted = True
                span.set_attribute("deleted", deleted)
                self.send_response(204 if deleted else 404)
                self.end_headers()

            self._handle(span_name, handle)

        def do_DELETE(self):
            if not self._allowed():
                return
            connection_id = self._path_id("/modbus/connections/")
            if connection_id is not None:
                self._delete(
                    "api.delete_modbus_connection",
                    lambda body: handle_modbus_delete_connection(modbus_manager, body),
                    connection_id,
                )
                return

            connection_id = self._path_id("/connections/")
            if connection_id is not None:
                self._delete("api.delete_connection", lambda body: handle_delete_connection(manager, body), connection_id)
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


# Actions whose `id` names the thing to act on (a device or connection)
# rather than being only the caller's correlation id. It's still echoed on
# the result like any other command's `id`.
TARGETED_ACTIONS = {
    "set_device_status", "recheck_credentials", "update_connection", "delete_connection",
    "modbus_update_connection", "modbus_delete_connection", "set_credential", "get_job", "connection_alarms",
    "connection_schedules", "write_schedule", "write_calendar",
}


# The command channel's own fields, around every action's parameters: the
# action name, the correlation ids, and the sender's expiry. Stripped before
# dispatch for every action (Baler#140), except where one is a parameter:
# a targeted action's `id` names its device/connection/job, and point_write
# refuses a command that arrives after its `expires_at`.
ENVELOPE_FIELDS = ("action", "id", "request_id", "expires_at")
ENVELOPE_PARAMS = {"point_write": {"expires_at"}, "write_schedule": {"expires_at"}, "write_calendar": {"expires_at"}}


def command_body(action, payload: dict) -> dict:
    keep = set(ENVELOPE_PARAMS.get(action, ()))
    if action in TARGETED_ACTIONS:
        keep.add("id")
    return {k: v for k, v in payload.items() if k not in ENVELOPE_FIELDS or k in keep}


def _make_on_command(
    manager: ConnectionManager,
    loop: asyncio.AbstractEventLoop,
    site_id: str,
    instance_id: str,
    modbus_manager: ModbusConnectionManager | None = None,
    device_registry: DeviceRegistry | None = None,
    jobs: JobRunner | None = None,
    announce_registry=None,
):
    """multi-site.mdx's command channel: a {"action": ..., ...} message on
    cmd/{site_id}/{instance_id}/# dispatches to the same handler its own
    HTTP route calls, and the outcome is published to .../result so a
    command isn't fire-and-forget. Runs on paho's network thread (same
    blocking-call shape as an HTTP handler's worker thread) - one command
    processes at a time, which is fine for scans you don't want overlapping.

    The subscription covers our own .../result and .../status topics too
    (MQTT 3.1.1 echoes a client's own publishes back to it), so those are
    skipped - otherwise every result, carrying its `action`, would be
    dispatched again as a new command, forever.
    """
    if jobs is None:
        jobs = JobRunner(manager, modbus_manager, device_registry)
    handlers = {
        "start_job": lambda body: handle_start_job(jobs, body),
        "get_job": lambda body: handle_get_job(jobs, body),
        "list_jobs": lambda body: handle_list_jobs(jobs, body),
        "discover": lambda body: handle_discover(manager, body),
        "discover_routers": lambda body: handle_discover_routers(manager, body),
        "network_status": lambda body: handle_network_status(manager, body),
        "learn": lambda body: handle_learn(manager, body),
        "read_schedules": lambda body: handle_read_schedules(manager, body),
        "connection_schedules": lambda body: handle_connection_schedules(manager, body),
        "modbus_discover": lambda body: handle_modbus_discover(body),
        "point_write": lambda body: handle_point_write(manager, body, modbus_manager),
        "write_schedule": lambda body: handle_write_schedule(manager, body),
        "write_calendar": lambda body: handle_write_calendar(manager, body),
        "list_devices": lambda body: handle_list_devices(device_registry, body),
        "list_announced": lambda body: handle_list_announced(announce_registry, body),
        "set_device_status": lambda body: handle_set_device_status(device_registry, body),
        "add_device": lambda body: handle_add_device(device_registry, manager, modbus_manager, body),
        "recheck_credentials": lambda body: handle_recheck_credentials(device_registry, body),
        "list_connections": lambda body: handle_list_connections(manager, body),
        "connection_alarms": lambda body: handle_connection_alarms(manager, body),
        "create_connection": lambda body: handle_create_connection(manager, body),
        "update_connection": lambda body: handle_update_connection(manager, body),
        "delete_connection": lambda body: handle_delete_connection(manager, body),
        "modbus_list_connections": lambda body: handle_modbus_list_connections(modbus_manager, body),
        "modbus_create_connection": lambda body: handle_modbus_create_connection(modbus_manager, body),
        "modbus_update_connection": lambda body: handle_modbus_update_connection(modbus_manager, body),
        "modbus_delete_connection": lambda body: handle_modbus_delete_connection(modbus_manager, body),
        "modbus_test_read": lambda body: handle_modbus_test_read(modbus_manager, body),
        "set_credential": lambda body: handle_set_credential(manager, modbus_manager, device_registry, body),
    }
    result_topic = f"cmd/{site_id}/{instance_id}/result"
    own_topics = {result_topic, status_topic(site_id, instance_id)}

    def on_command(client, userdata, msg):
        if msg.topic in own_topics:
            return
        with tracer.start_as_current_span("api.on_command") as span:
            span.set_attribute("topic", msg.topic)
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("command must be a JSON object")  # noqa: TRY004 - caught below as malformed
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                span.set_attribute("error", str(exc))
                print(f"malformed command on {msg.topic}: {exc!r}")
                return
            action = payload.get("action")
            span.set_attribute("action", str(action))
            # `request_id` is the caller's correlation id, echoed on the
            # result so concurrent callers can tell their outcomes apart.
            # Older callers (Baler's point writes before it) correlate on
            # `id`, which is still echoed - but for TARGETED_ACTIONS `id`
            # names the device/connection, so those callers must send
            # request_id.
            request_id = payload.get("request_id")
            legacy_id = payload.get("id")
            handler = handlers.get(action) if isinstance(action, str) else None
            if handler is None:
                outcome = {"ok": False, "error": f"unknown action {action!r}"}
            else:
                body = command_body(action, payload)
                try:
                    outcome = {"ok": True, "result": _run(handler(body), loop)}
                except Exception as exc:  # noqa: BLE001 - one bad command must not crash the mqtt loop thread
                    span.set_attribute("error", str(exc))
                    outcome = {"ok": False, "error": str(exc) if not isinstance(exc, KeyError) else f"missing {exc}"}
                except ErrorRejectAbortNack as exc:
                    # bacpypes3's Error/Reject/Abort are BaseExceptions: past
                    # `except Exception` they'd escape into paho's network thread
                    span.set_attribute("error", str(exc))
                    outcome = {"ok": False, "error": f"the device answered: {exc}"}
            outcome["action"] = action
            if legacy_id is not None:
                outcome["id"] = legacy_id
            if request_id is not None:
                outcome["request_id"] = request_id
            client.publish(result_topic, json.dumps(outcome), qos=1)

    return on_command


# --- heartbeat (cmd/{site}/{instance}/status) ---

# Seconds between status heartbeats; a module constant so tests can shrink it.
STATUS_INTERVAL = 60.0


def status_topic(site_id: str, instance_id: str) -> str:
    return f"cmd/{site_id}/{instance_id}/status"


def package_version() -> str:
    try:
        return importlib.metadata.version("windrow")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def offline_payload(site_id: str, instance_id: str) -> str:
    return json.dumps({"online": False, "site_id": site_id, "instance_id": instance_id})


def node_networks(address, modbus_cidrs) -> list[str]:
    """The subnets this node sits on, as CIDRs: its BACnet interface
    (--address, e.g. 192.168.1.20/24:47808) and any --modbus-scan-cidr.
    The Connectors page prefills its subnet-sweep dialog from these
    (Baler#114). A bare address with no mask, or anything unparsable, is
    skipped rather than guessed at."""
    out: list[str] = []
    candidates = []
    if isinstance(address, str) and "/" in address:
        candidates.append(address.rsplit(":", 1)[0] if address.count(":") == 1 else address)
    candidates.extend(modbus_cidrs or [])
    for value in candidates:
        try:
            net = ipaddress.ip_interface(str(value).strip()).network
        except ValueError:
            continue
        if net.num_addresses > 1 and str(net) not in out:
            out.append(str(net))
    return out


def _network_summary(manager) -> dict:
    """Mode and, for a foreign device, whether the BBMD took the registration
    - the one BBMD fact worth seeing from Baler without asking."""
    status = bacnet_network.network_status(getattr(manager, "app", None))
    return {k: status[k] for k in ("mode", "bbmd", "registration") if k in status}


def build_status(
    site_id: str, instance_id: str, started_at: str, manager, modbus_manager, device_registry, networks=None,
    announce_registry=None,
) -> dict:
    devices = device_registry.list()
    extra = announce_registry.summary() if announce_registry else {}
    return {**extra,
        "online": True,
        "site_id": site_id,
        "instance_id": instance_id,
        "version": package_version(),
        "started_at": started_at,
        "ts": _now_iso(),
        "bacnet_connections": len(manager.connections),
        "bacnet_network": _network_summary(manager),
        "bacnet_requests": manager.read_stats(),
        "modbus_connections": len(modbus_manager.connections),
        "spool": mqtt_sink.spool_stats(),
        "envelope": mqtt_sink.batch_stats(),
        "devices": len(devices),
        "pending_devices": sum(1 for d in devices if d.get("status") == "pending"),
        "networks": list(networks or []),
    }


async def run_status_heartbeat(client, topic: str, build, wake: asyncio.Event | None = None) -> None:
    """Retained, so Baler (or anyone) subscribing later sees this instance's
    last state at once rather than waiting out an interval; the matching
    last will flips it to offline if the connection dies. `wake` lets a
    reconnect re-announce straight away - while disconnected the broker
    will have published the will, and "offline" shouldn't stand for up to
    a minute after the edge is back."""
    wake = wake or asyncio.Event()
    while True:
        wake.clear()
        try:
            client.publish(topic, json.dumps(build()), qos=1, retain=True)
        except Exception as exc:  # noqa: BLE001 - a failed heartbeat must not end the heartbeat
            print(f"status heartbeat failed: {exc!r}")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout=STATUS_INTERVAL)


def publish_offline(client, site_id: str, instance_id: str, timeout: float = 2.0) -> None:
    """On clean shutdown: the will only fires on an unclean disconnect, so
    say it ourselves, and wait (briefly) for it to leave before the network
    loop stops."""
    try:
        info = client.publish(status_topic(site_id, instance_id), offline_payload(site_id, instance_id), qos=1, retain=True)
        info.wait_for_publish(timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - shutting down regardless; the will covers it
        print(f"couldn't publish offline status: {exc!r}")


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
    # an Application that also takes EventNotifications (Baler#164)
    app = bacnet_alarms.WindrowApplication.from_args(args)
    await asyncio.sleep(1)  # let transport setup finish, same as bridge.py
    if getattr(args, "foreign", None):
        print(f"BACnet: foreign device of BBMD {args.foreign}, ttl {args.ttl}s - "
              f"registration {bacnet_network.network_status(app).get('registration')}")
    elif getattr(args, "bbmd", None) is not None:
        print(f"BACnet: acting as a BBMD, BDT {args.bbmd or '(no peers)'}")

    loop = asyncio.get_event_loop()

    # multi-site.mdx's command channel: only meaningful once this process
    # has a stable site/instance identity - a bare `windrow` run with neither
    # set behaves exactly as before (fresh session, no command topic).
    command_topic = f"cmd/{args.site_id}/{args.instance_id}/#" if args.site_id else None
    heartbeat_wake = asyncio.Event()

    def _resubscribe_commands(client, userdata, flags, reason_code, properties):
        client.subscribe(command_topic, qos=1)
        loop.call_soon_threadsafe(heartbeat_wake.set)

    mqtt_client = mqtt_sink.connect(
        args.mqtt_host,
        args.mqtt_port,
        client_id=f"windrow-{args.site_id}-{args.instance_id}" if args.site_id else None,
        clean_session=False if args.site_id else None,
        username=args.mqtt_username,
        password=args.mqtt_password,
        on_connect_extra=_resubscribe_commands if command_topic else None,
        tls=args.mqtt_tls,
        will=(status_topic(args.site_id, args.instance_id), offline_payload(args.site_id, args.instance_id))
        if command_topic else None,
    )
    manager = ConnectionManager(
        app, mqtt_client, args.state_file,
        site_id=args.site_id, instance_id=args.instance_id if args.site_id else None,
    )
    app.event_listeners.append(manager.on_event_notification)
    await manager.start()

    modbus_manager = ModbusConnectionManager(
        mqtt_client, args.modbus_state_file,
        site_id=args.site_id, instance_id=args.instance_id if args.site_id else None,
    )
    await modbus_manager.start()
    # the outage spool holds WINDROW_SPOOL_OUTAGE_HOURS of what these poll
    mqtt_sink.set_reading_rate(lambda: manager.reading_rate() + modbus_manager.reading_rate())

    device_registry = DeviceRegistry(args.devices_state_file)
    await device_registry.start()

    # Device announce (windrow.announce): devices say "I am here" over UDP,
    # and connections that name them (device_name) follow their address.
    # DNS names are followed too, whether or not the listener is on.
    announce_registry = announce.AnnounceRegistry(
        args.announce_state_file, interval=args.announce_interval,
        allow=[ipaddress.ip_network(c, strict=False) for c in args.announce_allow or []],
        key=os.environ.get("WINDROW_ANNOUNCE_KEY") or None,
    )
    announce_registry.load()
    if args.announce_port:
        await announce.start_listener(announce_registry, manager, modbus_manager, args.announce_host, args.announce_port,
                                      on_change=lambda: loop.call_soon_threadsafe(heartbeat_wake.set))
    resolver_task = loop.create_task(announce.run_resolver(announce_registry, manager, modbus_manager, args.resolve_interval))
    resolver_task.add_done_callback(_crash_logger("device name resolver"))
    jobs = JobRunner(manager, modbus_manager, device_registry)
    started_at = _now_iso()
    networks = node_networks(getattr(args, "address", None), args.modbus_scan_cidr)

    def status_fn() -> dict:
        return build_status(
            args.site_id, args.instance_id, started_at, manager, modbus_manager, device_registry, networks,
            announce_registry=announce_registry,
        )

    if command_topic:
        mqtt_client.message_callback_add(
            command_topic,
            _make_on_command(manager, loop, args.site_id, args.instance_id, modbus_manager, device_registry, jobs,
                             announce_registry),
        )
        heartbeat_task = loop.create_task(run_status_heartbeat(
            mqtt_client,
            status_topic(args.site_id, args.instance_id),
            status_fn,
            heartbeat_wake,
        ))
        heartbeat_task.add_done_callback(_crash_logger("status heartbeat"))

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
        (args.host, args.port), make_handler(manager, modbus_manager, device_registry, loop, status_fn, jobs, announce_registry)
    )
    server.daemon_threads = True  # unset default is False - a hung request would otherwise block shutdown

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    print(f"BACnet/Modbus discovery/connections API listening on {args.host}:{args.port}")

    # SIGTERM (docker stop) otherwise kills the process without running the
    # finally below - no offline status, no Modbus client cleanup.
    stop = asyncio.Event()
    with contextlib.suppress(NotImplementedError, RuntimeError):
        loop.add_signal_handler(signal.SIGTERM, stop.set)

    try:
        await stop.wait()
    finally:
        server.shutdown()
        # modbus_manager.close() first: an unclosed AsyncModbusTcpClient
        # leaves its own internal reconnect task alive, which stalls
        # asyncio.run()'s default task-cancellation-on-shutdown
        # indefinitely - confirmed live (see test_modbus_connection_manager.py).
        await modbus_manager.close()
        await jobs.close()
        if command_topic:
            publish_offline(mqtt_client, args.site_id, args.instance_id)
        mqtt_client.loop_stop()
        manager.save_marks()
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
    parser.add_argument(
        "--announce-port", type=int, default=int(os.environ.get("WINDROW_ANNOUNCE_PORT", "0")),
        help=f"UDP port to hear device announcements on (windrow.announce; {announce.DEFAULT_PORT} by convention). 0 = off",
    )
    parser.add_argument("--announce-host", default="0.0.0.0")
    parser.add_argument(
        "--announce-allow", action="append", default=None,
        help="Source subnet allowed to announce, e.g. 192.168.1.0/24. Repeatable. Unset: any source "
        "(set WINDROW_ANNOUNCE_KEY to require signed announcements).",
    )
    parser.add_argument("--announce-interval", type=float, default=30.0,
                        help="the senders' interval; a device is offline after missing 3")
    parser.add_argument("--announce-state-file", default="windrow-announced.json")
    parser.add_argument("--resolve-interval", type=float, default=300.0,
                        help="seconds between re-resolving connections' device_name by announcement or DNS")
    bacnet_network.add_network_args(parser)
    args = parser.parse_args()
    for attr in ("state_file", "modbus_state_file", "devices_state_file"):
        legacy.adopt_state_file(getattr(args, attr))

    tracing.init_tracing("windrow-api")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
