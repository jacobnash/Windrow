"""
Live against the real mock Modbus device and the real mosquitto broker -
same pub/sub proof pattern as test_connection_manager.py.
"""

import asyncio
import json
import time

import mqtt_test_util
import pytest

from windrow.modbus_connection_manager import ModbusConnectionManager


@pytest.mark.integration
def test_create_connection_polls_and_delete_stops_it(tmp_path, mock_modbus_device):
    topic_prefix = f"fbf/modbus-conn-mgr-test-{int(time.time())}"

    async def run():
        mqtt_client = mqtt_test_util.connect()
        manager = ModbusConnectionManager(mqtt_client, str(tmp_path / "modbus-connections.json"))

        sub = mqtt_test_util.subscriber_client()
        received = {}
        sub.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            record = await manager.create_connection(
                host="127.0.0.1",
                port=mock_modbus_device,
                topic_prefix=topic_prefix,
                points={"zone-temp": 0},
                poll_interval=0.5,
            )

            value_topic = f"{topic_prefix}/zone-temp"
            for _ in range(30):
                sub.loop(timeout=0.1)
                if value_topic in received:
                    break
                await asyncio.sleep(0.1)

            assert value_topic in received
            assert received[value_topic]["value"] == 70

            deleted = await manager.delete_connection(record["id"])
            assert deleted is True
            assert await manager.list_connections() == []

            received.clear()
            await asyncio.sleep(1.0)  # > poll_interval - long enough for a stray publish to land
            for _ in range(10):
                sub.loop(timeout=0.1)

            assert value_topic not in received  # deletion actually stopped the poll task, not just the bookkeeping
        finally:
            sub.disconnect()
            await manager.close()
            mqtt_client.loop_stop()

    asyncio.run(run())


@pytest.mark.integration
def test_start_restores_persisted_connections_and_resumes_polling(tmp_path, mock_modbus_device):
    topic_prefix = f"fbf/modbus-conn-mgr-restore-test-{int(time.time())}"
    state_file = str(tmp_path / "modbus-connections.json")

    async def create_then_persist():
        mqtt_client = mqtt_test_util.connect()
        manager = ModbusConnectionManager(mqtt_client, state_file)
        try:
            await manager.create_connection(
                host="127.0.0.1",
                port=mock_modbus_device,
                topic_prefix=topic_prefix,
                points={"zone-temp": 0},
                poll_interval=0.5,
            )
        finally:
            # Closing here (not just stopping mqtt) matters: an unclosed
            # AsyncModbusTcpClient leaves its own internal reconnect task
            # alive, which stalls this asyncio.run() call's shutdown
            # indefinitely - confirmed live, not a defensive guess.
            await manager.close()
            mqtt_client.loop_stop()

    async def restore_and_observe():
        mqtt_client = mqtt_test_util.connect()
        manager = ModbusConnectionManager(mqtt_client, state_file)

        sub = mqtt_test_util.subscriber_client()
        received = {}
        sub.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            await manager.start()  # no new create call - restores from state_file alone

            value_topic = f"{topic_prefix}/zone-temp"
            for _ in range(30):
                sub.loop(timeout=0.1)
                if value_topic in received:
                    break
                await asyncio.sleep(0.1)

            assert value_topic in received
            assert len(await manager.list_connections()) == 1
        finally:
            sub.disconnect()
            await manager.close()
            mqtt_client.loop_stop()

    asyncio.run(create_then_persist())
    asyncio.run(restore_and_observe())


@pytest.mark.integration
def test_credential_round_trips_through_persistence(tmp_path, mock_modbus_device):
    """The stored record round-trips whatever credential shape it was
    given verbatim - encryption/redaction is the API layer's job
    (api.py + credentials.py), not this manager's."""
    state_file = str(tmp_path / "modbus-connections.json")

    async def run():
        mqtt_client = mqtt_test_util.connect()
        manager = ModbusConnectionManager(mqtt_client, state_file)
        try:
            record = await manager.create_connection(
                host="127.0.0.1",
                port=mock_modbus_device,
                topic_prefix="fbf/modbus-cred-test",
                points={"zone-temp": 0},
                credential={"username": "admin", "password_ciphertext": "opaque"},
            )
            assert record["credential"] == {"username": "admin", "password_ciphertext": "opaque"}

            updated = await manager.set_credential(record["id"], {"username": "admin2", "password_ciphertext": "opaque2"})
            assert updated["credential"]["username"] == "admin2"

            listed = await manager.list_connections()
            assert listed[0]["credential"]["username"] == "admin2"
        finally:
            await manager.close()
            mqtt_client.loop_stop()

    asyncio.run(run())
