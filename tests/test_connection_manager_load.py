"""Baler#47: hundreds of polling points (many to dead devices) must not
starve the event loop the HTTP API also runs on."""

import asyncio

import pytest

from windrow import connection_manager as cm
from windrow.connection_manager import ConnectionManager, failure_backoff


class NullMqtt:
    def is_connected(self):
        return True

    def publish(self, *a, **k):
        pass


class SlowApp:
    """read_property takes 50ms; tracks the most reads ever in flight."""

    def __init__(self, fail=False):
        self.in_flight = 0
        self.max_in_flight = 0
        self.calls = 0
        self.fail = fail

    async def read_property(self, address, obj, prop):
        self.calls += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0.05)
            if self.fail:
                raise TimeoutError("no response")
            return 1.0
        finally:
            self.in_flight -= 1


def test_failure_backoff_doubles_and_caps():
    assert failure_backoff(5.0, 0) == 5.0
    assert failure_backoff(5.0, 1) == 10.0
    assert failure_backoff(5.0, 3) == 40.0
    assert failure_backoff(5.0, 50) == cm.MAX_FAILURE_BACKOFF


def test_reads_are_capped_and_the_loop_stays_responsive(tmp_path, monkeypatch):
    monkeypatch.setattr(cm, "MAX_CONCURRENT_READS", 4)

    async def run():
        app = SlowApp()
        manager = ConnectionManager(app, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c1", "device_address": "10.0.0.9", "topic_prefix": "fbf/x", "poll_interval": 0.05,
                  "points": [{"object_identifier": f"analog-value,{i}", "label": f"p{i}"} for i in range(200)]}
        task = asyncio.get_event_loop().create_task(manager._poll_loop(record))
        # what an API call does: a trivial coroutine on the same loop
        lags = []
        for _ in range(10):
            t = asyncio.get_event_loop().time()
            await manager.list_connections()
            await asyncio.sleep(0.02)
            lags.append(asyncio.get_event_loop().time() - t)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return app, lags

    app, lags = asyncio.run(run())
    assert app.calls > 0
    assert app.max_in_flight <= 4  # never more than the cap, across 200 points
    assert max(lags) < 0.5  # the loop still answers promptly


def test_dead_device_backs_off(tmp_path, monkeypatch):
    monkeypatch.setattr(cm, "MAX_FAILURE_BACKOFF", 10.0)

    async def run():
        app = SlowApp(fail=True)
        manager = ConnectionManager(app, NullMqtt(), str(tmp_path / "c.json"))
        record = {"id": "c1", "device_address": "10.0.0.9", "topic_prefix": "fbf/x", "poll_interval": 0.05,
                  "points": [{"object_identifier": "analog-value,1", "label": "p1"}]}
        task = asyncio.get_event_loop().create_task(manager._poll_loop(record))
        await asyncio.sleep(1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return app.calls

    # without back-off a 0.05s interval + 0.05s read is ~10 tries/s; with it
    # the waits go 0.1, 0.2, 0.4, 0.8... so only a handful in a second
    assert asyncio.run(run()) <= 6
