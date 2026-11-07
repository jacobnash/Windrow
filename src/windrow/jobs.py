"""
Background jobs for the slow actions (Baler#139): a BACnet discover, a
point-list learn and a Modbus subnet sweep can take minutes, far longer
than any one HTTP request or command-channel round trip should wait (every
handler gives up after api.CALL_TIMEOUT, 15 s - a /24 sweep failed there).

start_job {job: discover|learn|modbus_discover, ...that action's fields}
returns at once with a job id; the caller polls get_job {id} for progress
and the result. Over HTTP that's POST /jobs + GET /jobs/{id}; over the
command channel, actions of the same names. A finished discover or
sweep also records what it found in the device registry, the way the
periodic scans do, so the devices show up as found.

Jobs live in memory only (a restart forgets them; the caller sees "no such
job" and can start again). The newest MAX_JOBS are kept; one of each kind
runs at a time, and starting a second answers with the one already running
rather than sweeping the network twice.
"""

import asyncio
import ipaddress
import time
import uuid

from windrow import modbus_discovery, tracing

tracer = tracing.get_tracer(__name__)

KINDS = ("discover", "learn", "modbus_discover")
MAX_JOBS = 20
# A job that runs longer than this is cancelled and reported failed.
MAX_RUNTIME = 30 * 60.0


class JobRunner:
    def __init__(self, manager, modbus_manager, device_registry, now=time.time):
        self.manager = manager
        self.modbus_manager = modbus_manager
        self.device_registry = device_registry
        self._now = now
        self.jobs: dict[str, dict] = {}
        self.tasks: dict[str, asyncio.Task] = {}

    # --- public, called on the loop (api.py's handlers) ---

    async def start(self, body: dict) -> dict:
        kind = body.get("job")
        if kind not in KINDS:
            raise ValueError(f"job must be one of {', '.join(KINDS)}, got {kind!r}")
        params = {k: v for k, v in body.items() if k != "job"}
        if kind == "learn":
            # fail fast on a bad body rather than in the background
            for key in ("device_address", "device_instance"):
                if key not in params:
                    raise KeyError(key)
        if kind == "modbus_discover":
            if "cidr" not in params:
                raise KeyError("cidr")
            ipaddress.ip_network(params["cidr"], strict=False)  # ValueError -> 400
        running = next((j for j in self.jobs.values() if j["job"] == kind and j["state"] == "running"), None)
        if running is not None and kind != "learn":
            return self.view(running)
        job = {
            "id": uuid.uuid4().hex, "job": kind, "params": params, "state": "running",
            "started_at": self._now(), "finished_at": None, "progress": None, "result": None, "error": None,
        }
        self.jobs[job["id"]] = job
        self._trim()
        task = asyncio.get_event_loop().create_task(self._run(job))
        self.tasks[job["id"]] = task
        return self.view(job)

    def get(self, job_id) -> dict | None:
        job = self.jobs.get(job_id) if isinstance(job_id, str) else None
        return self.view(job) if job else None

    def list(self) -> list[dict]:
        return [self.view(j, with_result=False) for j in sorted(self.jobs.values(), key=lambda j: j["started_at"])]

    async def close(self) -> None:
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()

    @staticmethod
    def view(job: dict, with_result: bool = True) -> dict:
        return {k: v for k, v in job.items() if k != "result" or with_result}

    # --- internals ---

    def _trim(self) -> None:
        finished = sorted((j for j in self.jobs.values() if j["state"] != "running"), key=lambda j: j["started_at"])
        while len(self.jobs) > MAX_JOBS and finished:
            gone = finished.pop(0)
            self.jobs.pop(gone["id"], None)
            self.tasks.pop(gone["id"], None)

    async def _run(self, job: dict) -> None:
        with tracer.start_as_current_span("jobs.run") as span:
            span.set_attribute("job", job["job"])
            try:
                job["result"] = await asyncio.wait_for(self._work(job), MAX_RUNTIME)
                job["state"] = "done"
            except asyncio.CancelledError:
                job["state"], job["error"] = "failed", "cancelled"
                raise
            except asyncio.TimeoutError:
                job["state"], job["error"] = "failed", f"gave up after {MAX_RUNTIME:g} s"
            except Exception as exc:  # noqa: BLE001 - a failed job reports its error, never crashes the loop
                job["state"], job["error"] = "failed", str(exc) or exc.__class__.__name__
                span.set_attribute("error", job["error"])
            finally:
                job["finished_at"] = self._now()
                self.tasks.pop(job["id"], None)

    async def _work(self, job: dict):
        params = job["params"]
        if job["job"] == "learn":
            return await self.manager.learn(params["device_address"], params["device_instance"])
        if job["job"] == "modbus_discover":
            def progress(done: int, total: int) -> None:
                job["progress"] = {"done": done, "total": total}

            found = await modbus_discovery.sweep(
                params["cidr"],
                port=params.get("port", modbus_discovery.DEFAULT_PORT),
                concurrency=params.get("concurrency", modbus_discovery.DEFAULT_CONCURRENCY),
                timeout=params.get("timeout", modbus_discovery.DEFAULT_TIMEOUT),
                progress=progress,
            )
            if self.device_registry is not None and self.modbus_manager is not None:
                await self.device_registry.reconcile_modbus(found, list(self.modbus_manager.connections.values()))
            return found
        # discover: no arguments is the periodic scan's own pass (broadcast,
        # then a directed Who-Is to each known connection's address).
        connections = list(self.manager.connections.values())
        if params or self.device_registry is None:
            found = await self.manager.discover(**params)
        else:
            found = await self.device_registry.scan_bacnet(self.manager.app, connections)
        if self.device_registry is not None:
            await self.device_registry.reconcile_bacnet(found, connections)
        return found
