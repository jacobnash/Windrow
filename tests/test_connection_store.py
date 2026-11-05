import os
import tempfile

from windrow.connection_store import load, new_connection_id, save


def test_load_missing_file_returns_empty_list():
    with tempfile.TemporaryDirectory() as tmpdir:
        assert load(os.path.join(tmpdir, "connections.json")) == []


def test_save_and_load_round_trips():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "connections.json")
        connections = [
            {
                "id": new_connection_id(),
                "device_address": "192.168.128.63:47808",
                "device_instance": 3456,
                "topic_prefix": "fbf/mock-ahu-1-dynamic",
                "poll_interval": 5.0,
                "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
            }
        ]

        save(path, connections)

        assert load(path) == connections


def test_save_leaves_no_lingering_tmp_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "connections.json")
        save(path, [])
        assert set(os.listdir(tmpdir)) == {"connections.json"}


def test_new_connection_id_is_unique():
    assert new_connection_id() != new_connection_id()
