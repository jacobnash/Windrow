"""Baler#110: per-connection poll health, in memory beside each manager's
records - what the Connectors page shows as "OK, last value <time>" or the
error. No broker or device needed (fake clients, captured publishes)."""

import asyncio

import pytest

from windrow import api
from windrow import modbus_connection_manager as mcm
from windrow.poll_status import PollStatus


def test_states_and_reset():
    clock = iter(range(100, 200))
    st = PollStatus(now=lambda: next(clock))
    assert st.snapshot("c", 2)["state"] == "waiting"
    st.success("c", "a", published=True)
    st.success("c", "b", published=False)
    snap = st.snapshot("c", 2)
    assert snap["state"] == "ok" and snap["last_value_at"] == 100 and snap["last_success_at"] == 101
    st.failure("c", "a", "timeout")
    snap = st.snapshot("c", 2)
    assert snap["state"] == "partial" and snap["failing_points"] == ["a"] and snap["last_error"] == "timeout"
    st.failure("c", ["a", "b"], "can't connect")
    assert st.snapshot("c", 2)["state"] == "error"
    st.success("c", "a", published=True)
    st.success("c", "b", published=True)
    snap = st.snapshot("c", 2)
    assert snap["state"] == "ok" and snap["last_error"] == "can't connect"  # the last error is still reported
    st.reset("c")
    assert st.snapshot("c", 2)["state"] == "waiting"


def test_long_errors_are_trimmed():
    st = PollStatus()
    st.failure("c", "a", "x" * 5000)
    assert len(st.snapshot("c", 1)["last_error"]) == 300


def test_listing_adds_poll_status_but_record_stays_clean(tmp_path):
    mgr = mcm.ModbusConnectionManager(mqtt_client=None, state_file=str(tmp_path / "m.json"))
    record = {"id": "c1", "host": "h", "port": 502, "device_id": 1, "topic_prefix": "fbf/t",
              "poll_interval": 5, "points": {"a": 0}, "credential": None}
    mgr.connections["c1"] = record
    mgr.status.failure("c1", "a", "boom")
    listed = asyncio.run(api.handle_modbus_list_connections(mgr))
    assert listed[0]["poll_status"]["state"] == "error"
    assert "poll_status" not in record


class Resp:
    def __init__(self, registers=None, error=False):
        self.registers = registers or []
        self._error = error

    def isError(self):
        return self._error

    def __str__(self):
        return "ExceptionResponse(illegal data address)"


class FakeClient:
    def __init__(self, up=True, error=False):
        self.up, self.error, self.closed, self.connected = up, error, False, False

    async def connect(self):
        self.connected = self.up
        return self.up

    async def read_holding_registers(self, address, count=1, device_id=1):
        return Resp(error=True) if self.error else Resp([address + 70])

    def close(self):
        self.closed = True
        self.connected = False


@pytest.fixture
def mgr(tmp_path, monkeypatch):
    published = []
    monkeypatch.setattr(mcm.mqtt_sink, "publish_reading", lambda c, prefix, label, value: published.append((label, value)))
    m = mcm.ModbusConnectionManager(mqtt_client=None, state_file=str(tmp_path / "m.json"))
    return m


def test_modbus_poll_loop_records_success_and_failure(mgr, monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(mgr, "_new_client", lambda record: client)
    record = {"id": "c1", "host": "h", "port": 502, "device_id": 1, "topic_prefix": "fbf/t",
              "poll_interval": 0.01, "points": {"a": 0}}

    async def run():
        await mgr._start_poll_task(record)
        for _ in range(200):
            if mgr.status.snapshot("c1", 1)["state"] == "ok":
                break
            await asyncio.sleep(0.01)
        assert mgr.status.snapshot("c1", 1)["last_value_at"] is not None
        client.error = True
        for _ in range(200):
            if mgr.status.snapshot("c1", 1)["state"] == "error":
                break
            await asyncio.sleep(0.01)
        snap = mgr.status.snapshot("c1", 1)
        assert snap["state"] == "error" and "register 0" in snap["last_error"]
        await mgr.close()

    asyncio.run(run())


def test_modbus_unreachable_marks_every_point_failing(mgr, monkeypatch):
    monkeypatch.setattr(mgr, "_new_client", lambda record: FakeClient(up=False))
    record = {"id": "c1", "host": "h", "port": 502, "device_id": 1, "topic_prefix": "fbf/t",
              "poll_interval": 0.01, "points": {"a": 0, "b": 1}}

    async def run():
        await mgr._start_poll_task(record)
        for _ in range(200):
            if mgr.status.snapshot("c1", 2)["state"] == "error":
                break
            await asyncio.sleep(0.01)
        snap = mgr.status.snapshot("c1", 2)
        assert snap["state"] == "error" and snap["failing_count"] == 2 and "can't connect" in snap["last_error"]
        await mgr.close()

    asyncio.run(run())


def test_test_read_returns_value_and_closes(mgr, monkeypatch):
    client = FakeClient()
    monkeypatch.setattr(mgr, "_new_client", lambda record: client)
    out = asyncio.run(mgr.test_read("10.0.0.5", 3, port=502, device_id=2))
    assert out == {"host": "10.0.0.5", "port": 502, "device_id": 2, "register": 3, "value": 73}
    assert client.closed


@pytest.mark.parametrize(("client", "message"), [
    (FakeClient(up=False), "can't connect"),
    (FakeClient(error=True), "refused register 3"),
])
def test_test_read_errors_say_what_went_wrong(mgr, monkeypatch, client, message):
    monkeypatch.setattr(mgr, "_new_client", lambda record: client)
    with pytest.raises(ValueError, match=message):
        asyncio.run(mgr.test_read("10.0.0.5", 3))
    assert client.closed


def test_test_read_validates_input(mgr):
    with pytest.raises(ValueError):
        asyncio.run(mgr.test_read("10.0.0.5", 70000))


def test_test_read_against_the_mock_device(tmp_path, mock_modbus_device):
    m = mcm.ModbusConnectionManager(mqtt_client=None, state_file=str(tmp_path / "m.json"))
    out = asyncio.run(m.test_read("127.0.0.1", 0, port=mock_modbus_device))
    assert out["value"] == 70
