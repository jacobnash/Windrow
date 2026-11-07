"""Baler#139: slow actions (discover, learn, a Modbus sweep) run as
background jobs - start_job answers at once, get_job reports progress and
the result - so a /24 sweep isn't cut off by the 15 s request timeout. No
broker or network: the sweep and the managers are faked."""

import asyncio
from types import SimpleNamespace

import pytest

from windrow import jobs as jobs_mod
from windrow import modbus_discovery


class Registry:
    def __init__(self):
        self.modbus, self.bacnet = [], []

    async def reconcile_modbus(self, devices, connections):
        self.modbus.append(devices)

    async def reconcile_bacnet(self, devices, connections):
        self.bacnet.append(devices)

    async def scan_bacnet(self, app, connections):
        return [{"device_instance": 1, "address": "10.0.0.1", "vendor_id": 7}]


def _runner(registry=None):
    manager = SimpleNamespace(connections={}, app=None)

    async def learn(address, instance):
        await asyncio.sleep(0.01)
        return [{"object_identifier": "analog-input,0", "label": "t"}]

    async def discover(**kw):
        return [{"device_instance": 2, "address": kw.get("address"), "vendor_id": 1}]

    manager.learn, manager.discover = learn, discover
    return jobs_mod.JobRunner(manager, SimpleNamespace(connections={}), registry or Registry())


async def _wait_done(runner, job_id):
    for _ in range(500):
        job = runner.get(job_id)
        if job["state"] != "running":
            return job
        await asyncio.sleep(0.01)
    raise AssertionError("job never finished")


def test_sweep_runs_in_the_background_with_progress_and_records_devices(monkeypatch):
    gate = asyncio.Event()

    async def fake_sweep(cidr, port=502, concurrency=64, timeout=1.0, progress=None):
        progress(0, 254)
        progress(100, 254)
        await gate.wait()
        progress(254, 254)
        return [{"host": "10.0.0.30", "port": 502}]

    monkeypatch.setattr(jobs_mod.modbus_discovery, "sweep", fake_sweep)
    registry = Registry()
    runner = _runner(registry)

    async def run():
        job = await runner.start({"job": "modbus_discover", "cidr": "10.0.0.0/24"})
        assert job["state"] == "running" and job["job"] == "modbus_discover"
        await asyncio.sleep(0.01)
        assert runner.get(job["id"])["progress"] == {"done": 100, "total": 254}
        # a second start while one runs answers with the running one
        again = await runner.start({"job": "modbus_discover", "cidr": "10.0.0.0/24"})
        assert again["id"] == job["id"]
        gate.set()
        done = await _wait_done(runner, job["id"])
        assert done["state"] == "done" and done["result"] == [{"host": "10.0.0.30", "port": 502}]
        assert done["progress"] == {"done": 254, "total": 254} and done["finished_at"] is not None
        assert registry.modbus == [[{"host": "10.0.0.30", "port": 502}]]
        assert "result" not in runner.list()[0]

    asyncio.run(run())


def test_learn_and_discover_jobs():
    registry = Registry()
    runner = _runner(registry)

    async def run():
        learn = await runner.start({"job": "learn", "device_address": "10.0.0.5", "device_instance": 5})
        assert (await _wait_done(runner, learn["id"]))["result"][0]["label"] == "t"
        scan = await runner.start({"job": "discover"})
        assert (await _wait_done(runner, scan["id"]))["result"][0]["vendor_id"] == 7  # the periodic-scan pass
        directed = await runner.start({"job": "discover", "address": "10.0.0.9"})
        assert (await _wait_done(runner, directed["id"]))["result"][0]["address"] == "10.0.0.9"
        assert len(registry.bacnet) == 2

    asyncio.run(run())


def test_a_failing_job_reports_its_error(monkeypatch):
    async def boom(*a, **kw):
        raise OSError("network unreachable")

    monkeypatch.setattr(jobs_mod.modbus_discovery, "sweep", boom)
    runner = _runner()

    async def run():
        job = await runner.start({"job": "modbus_discover", "cidr": "10.0.0.0/30"})
        done = await _wait_done(runner, job["id"])
        assert done["state"] == "failed" and "network unreachable" in done["error"]

    asyncio.run(run())


@pytest.mark.parametrize("body", [{"job": "rm"}, {"job": "learn", "device_address": "x"}, {"job": "modbus_discover"},
                                  {"job": "modbus_discover", "cidr": "nope"}])
def test_bad_starts_fail_at_once(body):
    with pytest.raises((ValueError, KeyError)):
        asyncio.run(_runner().start(body))


def test_old_finished_jobs_are_trimmed(monkeypatch):
    monkeypatch.setattr(jobs_mod, "MAX_JOBS", 3)
    runner = _runner()

    async def run():
        ids = []
        for _ in range(5):
            job = await runner.start({"job": "learn", "device_address": "a", "device_instance": 1})
            await _wait_done(runner, job["id"])
            ids.append(job["id"])
        assert len(runner.jobs) == 3 and runner.get(ids[0]) is None and runner.get(ids[-1]) is not None

    asyncio.run(run())


def test_sweep_reports_progress_per_host(monkeypatch):
    async def probe(host, port, timeout):
        return {"host": host, "port": port} if host.endswith(".2") else None

    monkeypatch.setattr(modbus_discovery, "_probe_one", probe)
    seen = []
    found = asyncio.run(modbus_discovery.sweep("10.0.0.0/29", progress=lambda d, t: seen.append((d, t))))
    assert found == [{"host": "10.0.0.2", "port": 502}]
    assert seen[0] == (0, 6) and seen[-1] == (6, 6) and len(seen) == 7
