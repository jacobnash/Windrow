"""Baler#128 end to end against pymodbus's simulator (windrow.mock_modbus_device
in a subprocess, real AsyncModbusTcpClient): every function code and data
type the mock serves arrives decoded, scaled and with its unit, old
bare-address connections read exactly as before, and holding registers and
coils can be written through point_write - directly, over HTTP-shaped
api.handle_point_write, and over the command channel. No broker: publishes
go to a fake MQTT client."""

import asyncio
import json
import time

import pytest

from windrow import api
from windrow import mock_modbus_device as mock_mb
from windrow import modbus_connection_manager as mcm

PREFIX = "fbf/mock/modbus-ahu"


class FakeMqtt:
    def __init__(self):
        self.values: dict[str, object] = {}
        self.tags: dict[str, dict] = {}
        self.published: list[tuple[str, str]] = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))
        if payload in (b"", ""):
            self.values.pop(topic, None)
            self.tags.pop(topic, None)
        elif topic.endswith("/tags"):
            self.tags[topic] = json.loads(json.loads(payload)["value"])
        elif "/" in topic and not topic.startswith("cmd/"):
            self.values[topic] = json.loads(payload)["value"]

        class _R:
            rc = 0

        return _R()


async def _until(cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def _run_with_manager(tmp_path, port, body, points=None, site_id=None):
    async def run():
        mqtt = FakeMqtt()
        mgr = mcm.ModbusConnectionManager(mqtt, str(tmp_path / "m.json"), site_id=site_id,
                                          instance_id="i1" if site_id else None)
        try:
            record = await mgr.create_connection(
                host="127.0.0.1", port=port, topic_prefix=PREFIX,
                points=points or mock_mb.POINTS, poll_interval=0.2,
            )
            want = {f"{PREFIX}/{label}" for label in record["points"]}
            assert await _until(lambda: want <= set(mqtt.values)), sorted(want - set(mqtt.values))
            await body(mgr, mqtt, record)
        finally:
            await mgr.close()

    asyncio.run(run())


def test_every_type_reads_correctly_with_units(tmp_path, mock_modbus_device):
    async def body(mgr, mqtt, record):
        v = {topic.rsplit("/", 1)[1]: value for topic, value in mqtt.values.items()}
        assert v["zone_temp"] == 70 and isinstance(v["zone_temp"], int)  # bare address, as before
        assert v["fan_status"] == 0
        assert v["discharge_air_temp"] == 55.04  # float32 12.8 °C, scale 1.8 offset 32 -> °F
        assert v["zone_temp_sp"] == 72.0  # int16 720 at scale 0.1
        assert v["energy"] == 70000  # uint32, little word order
        assert v["outside_air_temp"] == -4.5  # input register, signed, scaled
        assert v["fan_cmd"] is True  # coil
        assert v["filter_alarm"] is False  # discrete input

        assert mqtt.tags[f"{PREFIX}/discharge_air_temp/tags"]["unit"] == "°F"
        assert mqtt.tags[f"{PREFIX}/energy/tags"]["unit"] == "Wh"
        sp = mqtt.tags[f"{PREFIX}/zone_temp_sp/tags"]
        assert sp["writable"] is True and sp["unit"] == "°F" and "sensor" not in sp
        assert "writable" not in mqtt.tags[f"{PREFIX}/discharge_air_temp/tags"]
        assert mqtt.tags[f"{PREFIX}/equip/tags"]["points"] == list(mock_mb.POINTS)

    _run_with_manager(tmp_path, mock_modbus_device, body)


def test_legacy_connection_reads_unchanged(tmp_path, mock_modbus_device):
    """A stored {label: int} connection, restored from disk as-is."""
    state = tmp_path / "m.json"
    state.write_text(json.dumps([{
        "id": "old", "host": "127.0.0.1", "port": mock_modbus_device, "device_id": 1, "topic_prefix": PREFIX,
        "poll_interval": 0.2, "points": {"zone-temp": 0, "fan-status": 1}, "credential": None,
    }]))

    async def run():
        mqtt = FakeMqtt()
        mgr = mcm.ModbusConnectionManager(mqtt, str(state))
        try:
            await mgr.start()
            assert await _until(lambda: f"{PREFIX}/fan-status" in mqtt.values)
            assert mqtt.values[f"{PREFIX}/zone-temp"] == 70
            assert mqtt.values[f"{PREFIX}/fan-status"] == 0
            assert json.loads(state.read_text())[0]["points"] == {"zone-temp": 0, "fan-status": 1}
        finally:
            await mgr.close()

    asyncio.run(run())


def test_write_holding_register_with_scale_and_read_back(tmp_path, mock_modbus_device):
    async def body(mgr, mqtt, record):
        out = await mgr.write_point(f"{PREFIX}/zone_temp_sp", 68.5, 8)
        assert out["present_value"] == 68.5 and out["effective_level"] == 8
        assert mqtt.values[f"{PREFIX}/zone_temp_sp"] == 68.5
        # and the device really holds 685, as the next poll reads it
        mqtt.values.pop(f"{PREFIX}/zone_temp_sp")
        assert await _until(lambda: mqtt.values.get(f"{PREFIX}/zone_temp_sp") == 68.5)

    _run_with_manager(tmp_path, mock_modbus_device, body)


def test_write_each_register_type_and_word_order(tmp_path, mock_modbus_device):
    """Writes to the mock's spare holding registers 10-11 under every
    type/order, read back through the same point - FC6 for one register,
    FC16 for two."""
    cases = [
        ({"register": 10, "type": "int16"}, -300),
        ({"register": 10, "type": "uint16"}, 65000),
        ({"register": 10, "type": "int16", "byte_order": "little"}, -2),
        ({"register": 10, "type": "int32"}, -100000),
        ({"register": 10, "type": "uint32", "word_order": "little"}, 4000000000),
        ({"register": 10, "type": "float32"}, 21.5),
        ({"register": 10, "type": "float32", "word_order": "little", "byte_order": "little"}, -0.25),
        ({"register": 10, "type": "int32", "word_order": "little", "scale": 0.01, "offset": -10}, 1234.56),
    ]
    points = {f"p{i}": {**spec, "writable": True} for i, (spec, _) in enumerate(cases)}

    async def body(mgr, mqtt, record):
        for i, (_, value) in enumerate(cases):
            out = await mgr.write_point(f"{PREFIX}/p{i}", value, 16)
            assert out["present_value"] == value, (cases[i], out)

    _run_with_manager(tmp_path, mock_modbus_device, body, points=points)


def test_write_coil(tmp_path, mock_modbus_device):
    async def body(mgr, mqtt, record):
        assert (await mgr.write_point(f"{PREFIX}/fan_cmd", False, 8))["present_value"] is False
        assert (await mgr.write_point(f"{PREFIX}/fan_cmd", 1, 8))["present_value"] is True

    _run_with_manager(tmp_path, mock_modbus_device, body)


def test_priority_levels_and_relinquish(tmp_path, mock_modbus_device):
    """No priority array on a Modbus device, so Windrow keeps one: the
    highest level wins, relinquishing it falls back to the next, and the
    levels survive a restart (they're on the stored connection)."""
    topic = f"{PREFIX}/zone_temp_sp"

    async def body(mgr, mqtt, record):
        assert (await mgr.write_point(topic, 70.0, 17))["present_value"] == 70.0
        assert (await mgr.write_point(topic, 65.0, 8))["present_value"] == 65.0
        out = await mgr.write_point(topic, 75.0, 10)  # lower priority than 8: stored, 8 still wins
        assert out["present_value"] == 65.0 and out["effective_level"] == 8
        out = await mgr.write_point(topic, None, 8)  # a timed override ending
        assert out["present_value"] == 75.0 and out["effective_level"] == 10
        stored = json.loads((tmp_path / "m.json").read_text())[0]["write_levels"]
        assert stored == {"zone_temp_sp": {"17": 70.0, "10": 75.0}}
        await mgr.write_point(topic, None, 10)
        assert (await mgr.write_point(topic, 71.0, 17))["present_value"] == 71.0
        with pytest.raises(mcm.PointWriteError, match="not relinquished"):
            await mgr.write_point(topic, None, 17)

    _run_with_manager(tmp_path, mock_modbus_device, body)


@pytest.mark.parametrize("topic, value, priority, kwargs, msg", [
    (f"{PREFIX}/discharge_air_temp", 1.0, 8, {}, "isn't writable"),
    (f"{PREFIX}/filter_alarm", True, 8, {}, "isn't writable"),
    (f"{PREFIX}/nope", 1.0, 8, {}, "no Modbus point"),
    (f"{PREFIX}/zone_temp_sp", 99999.0, 8, {}, "out of range"),
    (f"{PREFIX}/zone_temp_sp", "hot", 8, {}, "finite number"),
    (f"{PREFIX}/fan_cmd", 5, 8, {}, "true/false"),
    (f"{PREFIX}/zone_temp_sp", 70.0, 0, {}, "priority"),
    (f"{PREFIX}/zone_temp_sp", 70.0, True, {}, "priority"),
    (f"{PREFIX}/zone_temp_sp", 70.0, 8, {"expires_at": 1.0}, "expired"),
])
def test_bad_writes_are_refused_without_touching_the_device(tmp_path, mock_modbus_device, topic, value, priority,
                                                            kwargs, msg):
    async def body(mgr, mqtt, record):
        with pytest.raises(mcm.PointWriteError, match=msg):
            await mgr.write_point(topic, value, priority, **kwargs)
        assert "write_levels" not in mgr.connections[record["id"]]
        await asyncio.sleep(0.3)
        assert mqtt.values[f"{PREFIX}/zone_temp_sp"] == 72.0

    _run_with_manager(tmp_path, mock_modbus_device, body)


def test_point_write_routes_to_modbus_over_http_handler_and_command_channel(tmp_path, mock_modbus_device):
    """Baler sends the same point_write for any protocol; a topic a Modbus
    connection publishes goes to the Modbus manager. The equip record
    carries windrowSite/windrowInstance so Baler can route it at all."""

    class NoBacnet:
        async def write_point(self, *a, **k):
            raise AssertionError("routed to BACnet")

    async def body(mgr, mqtt, record):
        assert mqtt.tags[f"{PREFIX}/equip/tags"]["tags"] == {"windrowSite": "site-a", "windrowInstance": "i1"}
        out = await api.handle_point_write(NoBacnet(), {"topic": f"{PREFIX}/zone_temp_sp", "value": 66.0,
                                                        "priority": 8}, mgr)
        assert out["present_value"] == 66.0

        loop = asyncio.get_running_loop()
        on_command = api._make_on_command(NoBacnet(), loop, "site-a", "i1", modbus_manager=mgr)

        class Msg:
            topic = "cmd/site-a/i1/point-write"
            payload = json.dumps({"action": "point_write", "id": "r1", "request_id": "r1",
                                  "topic": f"{PREFIX}/fan_cmd", "value": False, "priority": 8,
                                  "expires_at": time.time() + 30}).encode()

        await asyncio.to_thread(on_command, mqtt, None, Msg)
        result = json.loads(next(p for t, p in mqtt.published if t == "cmd/site-a/i1/result"))
        assert result["ok"] is True, result
        assert result["result"]["present_value"] is False and result["request_id"] == "r1"

    _run_with_manager(tmp_path, mock_modbus_device, body, site_id="site-a")
