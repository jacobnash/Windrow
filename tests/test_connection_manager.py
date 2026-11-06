"""
Live against the real running mock device and the real mosquitto broker -
same pub/sub proof pattern the other bridge tests use.
"""

import asyncio
import json
import time

import mqtt_test_util
import pytest
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser

from windrow.connection_manager import ConnectionManager

DEVICE_ADDRESS = "192.168.128.63:47808"
DEVICE_INSTANCE = 3456


def _make_app(bind_port: int) -> Application:
    parser = SimpleArgumentParser()
    args = parser.parse_args(["--address", f"192.168.128.63/24:{bind_port}"])
    return Application.from_args(args)


@pytest.mark.integration
def test_create_connection_polls_and_delete_stops_it(tmp_path):
    topic_prefix = f"fbf/conn-mgr-test-{int(time.time())}"

    async def run():
        app = _make_app(47813)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))

        sub = mqtt_test_util.subscriber_client()
        received = {}
        sub.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            await asyncio.sleep(1)  # let the Application's transport finish setup

            record = await manager.create_connection(
                device_address=DEVICE_ADDRESS,
                device_instance=DEVICE_INSTANCE,
                topic_prefix=topic_prefix,
                points=[{"object_identifier": "analog-value,1", "label": "zone-temp"}],
                poll_interval=1.0,
            )

            value_topic = f"{topic_prefix}/zone-temp"
            for _ in range(30):
                sub.loop(timeout=0.1)
                if value_topic in received:
                    break
                await asyncio.sleep(0.1)

            assert value_topic in received
            assert isinstance(received[value_topic]["value"], float)

            deleted = await manager.delete_connection(record["id"])
            assert deleted is True
            assert await manager.list_connections() == []

            received.clear()
            await asyncio.sleep(1.5)  # > poll_interval - long enough for a stray publish to land
            for _ in range(10):
                sub.loop(timeout=0.1)

            assert value_topic not in received  # deletion actually stopped the poll task, not just the bookkeeping
        finally:
            sub.disconnect()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())


@pytest.mark.integration
def test_start_restores_persisted_connections_and_resumes_polling(tmp_path):
    topic_prefix = f"fbf/conn-mgr-restore-test-{int(time.time())}"
    state_file = str(tmp_path / "connections.json")

    async def create_then_persist():
        app = _make_app(47814)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, state_file)
        try:
            await asyncio.sleep(1)
            await manager.create_connection(
                device_address=DEVICE_ADDRESS,
                device_instance=DEVICE_INSTANCE,
                topic_prefix=topic_prefix,
                points=[{"object_identifier": "analog-value,1", "label": "zone-temp"}],
                poll_interval=1.0,
            )
        finally:
            mqtt_client.loop_stop()
            app.close()

    async def restore_and_observe():
        app = _make_app(47815)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, state_file)

        sub = mqtt_test_util.subscriber_client()
        received = {}
        sub.on_message = lambda c, u, msg: received.update({msg.topic: json.loads(msg.payload)})
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            await asyncio.sleep(1)
            await manager.start()  # no new API call - restores from state_file alone

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
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(create_then_persist())
    asyncio.run(restore_and_observe())


@pytest.mark.integration
def test_per_point_poll_interval_yields_different_real_cadences(tmp_path):
    """Two points on ONE connection, two different poll_intervals - proves
    each point is genuinely polled on its own schedule, not the
    connection's single shared interval."""
    topic_prefix = f"fbf/conn-mgr-interval-test-{int(time.time())}"

    async def run():
        app = _make_app(47817)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))

        sub = mqtt_test_util.subscriber_client()
        counts = {"fast": 0, "slow": 0}
        sub.on_message = lambda c, u, msg: counts.__setitem__(msg.topic.rsplit("/", 1)[-1], counts[msg.topic.rsplit("/", 1)[-1]] + 1)
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            await asyncio.sleep(1)

            await manager.create_connection(
                device_address=DEVICE_ADDRESS,
                device_instance=DEVICE_INSTANCE,
                topic_prefix=topic_prefix,
                points=[
                    {"object_identifier": "analog-value,1", "label": "fast", "poll_interval": 0.2},
                    {"object_identifier": "analog-value,1", "label": "slow", "poll_interval": 1.0},
                ],
                poll_interval=5.0,  # connection default - neither point should actually use this
            )

            end = time.monotonic() + 2.2
            while time.monotonic() < end:
                sub.loop(timeout=0.1)
                await asyncio.sleep(0.1)  # yield to the event loop, or the poll tasks never get scheduled

            # fast (0.2s) should have published roughly 5x more than slow (1.0s)
            # over the same ~2.2s window - loose bounds, not exact timing
            assert counts["fast"] >= 6
            assert counts["slow"] <= 4
            assert counts["fast"] > counts["slow"] * 2
        finally:
            sub.disconnect()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())


@pytest.mark.integration
def test_his_collect_cov_suppresses_unchanged_readings(tmp_path):
    """The mock device only changes its value once every 5s (mock_device.py's
    own INTERVAL) - polling faster than that with a COV threshold means most
    polls should be suppressed as unchanged, not republished."""
    topic_prefix = f"fbf/conn-mgr-cov-test-{int(time.time())}"

    async def run():
        app = _make_app(47818)
        mqtt_client = mqtt_test_util.connect()
        manager = ConnectionManager(app, mqtt_client, str(tmp_path / "connections.json"))

        sub = mqtt_test_util.subscriber_client()
        received = []
        # /tags is a real, separate message the connection now publishes once
        # at connection-creation time (bacnet_tagger auto-tagging) - excluded
        # here the same way every real consumer (mqtt_listener.py) already
        # distinguishes it from a reading, so this stays a count of readings.
        sub.on_message = lambda c, u, msg: None if msg.topic.endswith("/tags") else received.append(json.loads(msg.payload))
        sub.connect("localhost", 1883)
        sub.subscribe(f"{topic_prefix}/#")

        try:
            await asyncio.sleep(1)

            await manager.create_connection(
                device_address=DEVICE_ADDRESS,
                device_instance=DEVICE_INSTANCE,
                topic_prefix=topic_prefix,
                points=[
                    {
                        "object_identifier": "analog-value,1",
                        "label": "zone-temp-cov",
                        "poll_interval": 0.2,
                        "his_collect_cov": 0.5,
                    }
                ],
            )

            end = time.monotonic() + 2.0
            while time.monotonic() < end:
                sub.loop(timeout=0.1)
                await asyncio.sleep(0.1)  # yield to the event loop, or the poll task never gets scheduled

            # ~10 polls (0.2s x 2.0s) happened. The mock device's own 5s
            # change cycle isn't synchronized with when this test starts,
            # so a real device-side change can occasionally land inside
            # the window (this is exactly what COV is supposed to let
            # through) - the pure should_publish() logic already has
            # exact-math unit test coverage; this integration test's job
            # is proving the wiring suppresses most polls, not re-deriving
            # precise COV timing against the device's own unsynchronized
            # schedule.
            assert 1 <= len(received) <= 2
        finally:
            sub.disconnect()
            mqtt_client.loop_stop()
            app.close()

    asyncio.run(run())
