"""Baler#83: the Modbus poll loop never publishes a null reading, retries a
first connect that failed, and reconnects after the device drops - no
broker or device needed (fake client, captured publishes)."""

import asyncio

import pytest

from windrow import modbus_connection_manager as mcm


class FakeDevice:
    def __init__(self, up=True, value=70):
        self.up = up
        self.value = value
        self.connects = 0


class Resp:
    def __init__(self, registers=None, error=False):
        self.registers = registers or []
        self._error = error

    def isError(self):
        return self._error


class FakeClient:
    def __init__(self, device):
        self.device = device
        self.connected = False

    async def connect(self):
        self.device.connects += 1
        self.connected = self.device.up
        return self.connected

    async def read_holding_registers(self, address, count=1, device_id=1):
        if not self.device.up:
            self.connected = False
            raise ConnectionError("[Connection] Not connected")
        return Resp([self.device.value])

    def close(self):
        self.connected = False


@pytest.fixture
def harness(tmp_path, monkeypatch):
    device = FakeDevice()
    published = []
    monkeypatch.setattr(mcm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append((label, value)))
    monkeypatch.setattr(mcm, "RECONNECT_MAX", 0.05)
    mgr = mcm.ModbusConnectionManager(mqtt_client=None, state_file=str(tmp_path / "m.json"))
    monkeypatch.setattr(mgr, "_new_client", lambda record: FakeClient(device))
    return mgr, device, published


async def _wait(cond, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if cond():
            return True
        await asyncio.sleep(0.01)
    return False


def _record():
    return {"id": "c1", "host": "10.0.0.9", "port": 502, "device_id": 1, "topic_prefix": "fbf/t",
            "poll_interval": 0.01, "points": {"zone_temp": 0}}


def test_device_down_publishes_nothing_then_resumes(harness):
    mgr, device, published = harness

    async def run():
        await mgr._start_poll_task(_record())
        try:
            assert await _wait(lambda: len(published) >= 2)
            device.up = False
            await asyncio.sleep(0.05)
            published.clear()
            await asyncio.sleep(0.2)
            assert published == []  # no null readings while down
            device.value = 71
            device.up = True
            assert await _wait(lambda: ("zone_temp", 71) in published)
        finally:
            await mgr.close()

    asyncio.run(run())
    assert all(v is not None for _, v in published)


def test_first_connect_failure_is_retried(harness):
    mgr, device, published = harness
    device.up = False

    async def run():
        await mgr._start_poll_task(_record())
        try:
            assert await _wait(lambda: device.connects >= 2)
            assert published == []
            device.up = True
            assert await _wait(lambda: ("zone_temp", 70) in published)
        finally:
            await mgr.close()

    asyncio.run(run())


def test_device_exception_response_is_skipped_not_null(harness):
    mgr, device, published = harness

    async def bad_read(self, address, count=1, device_id=1):
        return Resp(error=True)

    FakeClient.read_holding_registers, orig = bad_read, FakeClient.read_holding_registers
    try:
        async def run():
            await mgr._start_poll_task(_record())
            try:
                await asyncio.sleep(0.1)
            finally:
                await mgr.close()

        asyncio.run(run())
    finally:
        FakeClient.read_holding_registers = orig
    assert published == []
    assert device.connects == 1  # an exception code isn't a lost session
