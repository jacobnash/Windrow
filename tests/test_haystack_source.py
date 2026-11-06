"""windrow.haystack_source against a live Haystack server: openhay's `hay
serve` loaded with an nhaystack-shaped station (tests/fixtures/
nhaystack_station.trio), so discovery, watches, history backfill and
write-back all go over real HTTP + SCRAM with no Niagara station.

Needs the `openhay` Python package and the `hay` binary (HAY_BIN, PATH, or
a sibling ../openhay checkout's target/). Skipped locally if either is
missing; under CI (CI=true) a missing dependency is a failure, so this can
never silently not run there."""

import datetime as dt
import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "nhaystack_station.trio"


def _require(cond: bool, why: str):
    if not cond:
        if os.environ.get("CI"):
            pytest.fail(f"required in CI: {why}")
        pytest.skip(why)


try:
    import openhay as oh
except ImportError:  # pragma: no cover
    oh = None


def _hay_bin() -> str | None:
    cand = [os.environ.get("HAY_BIN"), shutil.which("hay")]
    sib = Path(__file__).resolve().parents[2] / "openhay" / "target"
    cand += [str(sib / "debug" / "hay"), str(sib / "release" / "hay")]
    return next((c for c in cand if c and Path(c).is_file()), None)


@pytest.fixture(scope="module")
def station():
    _require(oh is not None, "openhay not installed (pip install -e '.[haystack]')")
    hay = _hay_bin()
    _require(hay is not None, "hay binary not found (cargo install --git https://github.com/jacobnash/openhay hay-cli)")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([hay, "serve", str(FIXTURE), "--addr", f"127.0.0.1:{port}", "--auth", "admin:pencil"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.05)
    yield f"http://127.0.0.1:{port}/api/station"
    proc.terminate()
    proc.wait()


class FakeMqtt:
    def __init__(self):
        self.published = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))

        class _R:
            rc = 0

        return _R()

    def values(self):
        return {t: json.loads(p)["value"] for t, p in self.published if not t.endswith("/tags")}


@pytest.fixture
def source(station):
    from windrow.haystack_source import HaystackSource

    client = oh.Client(station, user="admin", password="pencil")
    return HaystackSource(client, FakeMqtt(), "Station A", site_id="hq", instance_id="station-1")


def test_discovery_topics_tags_and_route(source):
    assert source.discover() == 4
    assert set(source.by_topic) == {
        "fbf/station-a/ahu1/sat", "fbf/station-a/ahu1/sat-sp", "fbf/station-a/ahu1/fan-cmd", "fbf/station-a/misc/oat",
    }
    source.publish_tags()
    pub = {t: json.loads(json.loads(p)["value"]) for t, p in source.mqtt.published}
    sat = pub["fbf/station-a/ahu1/sat/tags"]
    assert sat["sensor"] is True and sat["temp"] is True
    assert sat["axType"] == "control:NumericPoint" and sat["axHistoryId"] == "/Hospital/AHU1_SAT"
    assert sat["sourceId"] == "S.Hospital.AHU1.points.SAT"
    assert "curVal" not in sat and "id" not in sat and "equipRef" not in sat
    assert pub["fbf/station-a/ahu1/sat-sp/tags"]["writable"] is True
    equip = pub["fbf/station-a/ahu1/equip/tags"]
    assert equip["tags"]["windrowSite"] == "hq" and equip["tags"]["windrowInstance"] == "station-1"
    assert equip["tags"]["ahu"] is True and sorted(equip["points"]) == ["fan-cmd", "sat", "sat-sp"]


def test_watch_publishes_initial_values_then_only_changes(source, station):
    source.discover()
    assert source.poll_once() == 4
    vals = source.mqtt.values()
    assert vals["fbf/station-a/ahu1/sat"] == 55.2 and vals["fbf/station-a/ahu1/fan-cmd"] is False
    assert source.poll_once() == 0
    other = oh.Client(station, user="admin", password="pencil")  # someone else commands the station
    other.point_write("S.Hospital.AHU1.points.SATSP", 8, oh.Number(58.0), who="operator")
    assert source.poll_once() == 1
    assert source.mqtt.values()["fbf/station-a/ahu1/sat-sp"] == 58.0
    other.point_write("S.Hospital.AHU1.points.SATSP", 8, None)


def test_backfill_replays_history_with_timestamps(source, station):
    other = oh.Client(station, user="admin", password="pencil")
    tz = oh.haystack_tz("New_York")
    t1, t2 = dt.datetime(2024, 1, 2, 10, tzinfo=tz), dt.datetime(2024, 1, 2, 11, tzinfo=tz)
    other.his_write("S.Hospital.AHU1.points.SAT", [(t1, 54.0), (t2, 56.5)])
    source.discover()
    assert source.backfill("2024-01-02") == 2
    rows = [json.loads(p) for t, p in source.mqtt.published if t == "fbf/station-a/ahu1/sat"]
    assert [(r["value"], r["ts"]) for r in rows] == [(54.0, t1.timestamp()), (56.5, t2.timestamp())]


def _command(source, **body):
    replies = FakeMqtt()
    msg = SimpleNamespace(topic="cmd/hq/station-1/point-write", payload=json.dumps({"action": "point_write", **body}).encode())
    source.on_command(replies, None, msg)
    topic, payload = replies.published[-1]
    assert topic == "cmd/hq/station-1/result"
    return json.loads(payload)


def test_command_channel_write_lands_in_station_priority_array(source, station):
    source.discover()
    out = _command(source, id="w-1", topic="fbf/station-a/ahu1/sat-sp", value=60, priority=8, expires_at=time.time() + 30)
    assert out["ok"] and out["id"] == "w-1" and out["result"]["present_value"] == 60.0
    arr = oh.Client(station, user="admin", password="pencil").point_write_array("S.Hospital.AHU1.points.SATSP")
    assert arr.rows[7]["val"] == oh.Number(60.0)
    assert source.mqtt.values()["fbf/station-a/ahu1/sat-sp"] == 60.0
    assert _command(source, id="w-2", topic="fbf/station-a/ahu1/sat-sp", value=None, priority=8)["ok"]


@pytest.mark.parametrize("body, msg", [
    ({"topic": "fbf/station-a/misc/oat", "value": 1, "priority": 8}, "not writable"),
    ({"topic": "fbf/station-a/ahu1/nope", "value": 1, "priority": 8}, "no Haystack point"),
    ({"topic": "fbf/station-a/ahu1/sat-sp", "value": 1, "priority": 0}, "1-16"),
    ({"topic": "fbf/station-a/ahu1/sat-sp", "value": 1, "priority": 8, "expires_at": 1.0}, "expired"),
])
def test_command_refusals(source, body, msg):
    source.discover()
    out = _command(source, **body)
    assert not out["ok"] and msg in out["error"]


def test_falls_back_to_read_when_server_has_no_watches():
    from windrow.haystack_source import HaystackSource

    _require(oh is not None, "openhay not installed")

    class NoWatchClient:
        def read(self, flt):
            return [{"id": oh.Ref("p1"), "point": oh.MARKER, "dis": "P1", "curVal": oh.Number(1.0)}]

        def read_ids(self, ids):
            return [{"id": oh.Ref("p1"), "curVal": oh.Number(2.0)}]

        def watch_sub(self, *a):
            raise oh.HaystackError("unknown op: watchSub")

    s = HaystackSource(NoWatchClient(), FakeMqtt(), "plain")
    s.discover()
    assert s.poll_once() == 1 and s.mqtt.values()["fbf/plain/misc/p1"] == 2.0
    assert s.poll_once() == 0  # unchanged, and no further watchSub attempts
