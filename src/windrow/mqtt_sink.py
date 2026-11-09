"""
Shared MQTT publish logic for every bridge. The one thing a new protocol
bridge imports instead of re-deriving the envelope/publish logic each time
- see docs/adding-a-new-source.md.

Also owns the local spool: if the client isn't connected when a reading
is taken (the uplink from an on-site host to the broker is down, but this
process is still polling BACnet/Modbus locally), the reading is appended
to a JSONL file instead of dropped, and replayed on the next on_connect.
Idempotent replay relies on Baler's ingest_reading() being
idempotent on (point_uri, ts) - see baler's ingest.py.
"""

import json
import os
import threading
import time

import paho.mqtt.client as mqtt

from windrow import legacy

# Spool capacity (Baler#129): enough rows to cover an uplink outage of
# WINDROW_SPOOL_OUTAGE_HOURS at the rate this process takes readings (the
# connection managers report it through set_reading_rate), with 10% slack,
# never below MIN_SPOOL_ROWS and never above MAX_SPOOL_ROWS - the disk
# ceiling (a reading is ~100 bytes on disk, so 5M rows is ~500 MB). It was
# a fixed 10k rows: at 500 points on a 5 s poll that's 100 seconds.
SPOOL_OUTAGE_HOURS = float(legacy.env("WINDROW_SPOOL_OUTAGE_HOURS") or 24)
MAX_SPOOL_ROWS = int(legacy.env("WINDROW_SPOOL_MAX_ROWS") or 5_000_000)
MIN_SPOOL_ROWS = 10_000

_spool_lock = threading.Lock()
_reading_rate = None  # () -> readings per second, from the connection managers
_spool_rows: dict[str, int] = {}  # path -> rows in it, so an append doesn't re-read the file
_spool_dropped = 0


def set_reading_rate(rate) -> None:
    """Register a callable giving the readings per second this process
    takes - what the spool must hold per second of outage."""
    global _reading_rate
    _reading_rate = rate


def spool_capacity() -> int:
    rate = 0.0
    if _reading_rate is not None:
        try:
            rate = float(_reading_rate())
        except Exception as exc:  # noqa: BLE001 - a bad estimate falls back to the minimum, it doesn't stop spooling
            print(f"spool: reading rate unavailable ({exc!r}); using the minimum capacity")
    wanted = int(rate * SPOOL_OUTAGE_HOURS * 3600 * 1.1)
    return min(MAX_SPOOL_ROWS, max(MIN_SPOOL_ROWS, wanted))


def spool_stats() -> dict:
    """Rows waiting, the current capacity, and rows dropped since start."""
    path = _spool_path()
    with _spool_lock:
        rows = _count_rows(path)
    return {"rows": rows, "capacity": spool_capacity(), "dropped": _spool_dropped}


def _spool_path() -> str:
    return legacy.adopt_state_file(legacy.env("WINDROW_SPOOL_FILE") or "windrow-spool.jsonl")


def _read_spool(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                # A hard kill mid-append can only corrupt the last,
                # incomplete line - every prior line is already
                # newline-terminated and durable. Drop just this one.
                print(f"dropping corrupt spool line: {exc!r}")
    return records


def _write_spool(path: str, records: list[dict]) -> None:
    """Atomic write: temp file + os.replace (same pattern as
    connection_store.save) - only used for the cap-trim and post-flush
    clear, not the hot append path."""
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        f.writelines(json.dumps(record) + "\n" for record in records)
    os.replace(tmp_path, path)


def _count_rows(path: str) -> int:
    if path not in _spool_rows:
        if os.path.exists(path):
            with open(path) as f:
                _spool_rows[path] = sum(1 for line in f if line.strip())
        else:
            _spool_rows[path] = 0
    return _spool_rows[path]


def _spool_append(record: dict) -> None:
    """Append one reading. Over capacity, the oldest rows go - trimmed in one
    rewrite once the file is 10% over (not on every append, which made each
    append re-read the whole file), and logged: how many, and the time span
    lost."""
    global _spool_dropped
    path = _spool_path()
    with _spool_lock:
        rows = _count_rows(path)
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
        rows += 1
        _spool_rows[path] = rows
        capacity = spool_capacity()
        # 10% slack before a trim, except for a tiny (test-sized) cap
        threshold = int(capacity * 1.1) if capacity >= MIN_SPOOL_ROWS else capacity
        if rows > threshold:
            records = _read_spool(path)
            dropped = records[:-capacity]
            _write_spool(path, records[-capacity:])
            _spool_rows[path] = min(len(records), capacity)
            if dropped:
                _spool_dropped += len(dropped)
                points = len({(r.get("topic_prefix"), r.get("point")) for r in dropped})
                print(
                    f"spool full ({capacity} rows, sized for {SPOOL_OUTAGE_HOURS:g} h of outage): dropped the "
                    f"{len(dropped)} oldest reading(s) of {points} point(s), "
                    f"ts {dropped[0].get('ts')} to {dropped[-1].get('ts')}; {_spool_dropped} dropped since start"
                )


def _flush_spool(client: mqtt.Client) -> None:
    path = _spool_path()
    with _spool_lock:
        records = _read_spool(path)
        if not records:
            return
        for i, record in enumerate(records):
            if not client.is_connected():
                print(f"reconnect lost mid-flush, {len(records) - i} record(s) remain spooled")
                return
            _publish(client, record["topic_prefix"], record["point"], record["value"], record["ts"],
                     retain=record.get("retain", True), priority=record.get("priority"))
        os.remove(path)
        _spool_rows[path] = 0
        print(f"flushed {len(records)} spooled reading(s)")


def _on_connect(client, userdata, flags, reason_code, properties) -> None:
    _flush_spool(client)


def use_tls(port: int, tls: bool | None = None) -> bool:
    """TLS when asked (--mqtt-tls, or MQTT_TLS=1), or on 8883, MQTT's TLS
    port - an edge node publishing to Baler from a site (Baler#36)."""
    if tls is not None:
        return tls
    flag = os.environ.get("MQTT_TLS", "").strip().lower()
    if flag:
        return flag in ("1", "true", "yes", "on")
    return port == 8883


def connect(
    host: str,
    port: int,
    client_id: str | None = None,
    clean_session: bool | None = None,
    username: str | None = None,
    password: str | None = None,
    on_connect_extra=None,
    tls: bool | None = None,
    will: tuple[str, str] | None = None,
) -> mqtt.Client:
    """client_id/clean_session are only worth setting once this process has
    a stable identity (see api.py's --site-id) - a persistent session lets
    mosquitto queue messages (e.g. commands, see multi-site.mdx) while this
    client is offline instead of dropping them; without a stable identity
    every restart would get a fresh session anyway, so there's nothing to
    persist. on_connect_extra runs after the spool flush on every (re)connect
    - e.g. api.py uses it to resubscribe to its command topic. `will` is a
    (topic, payload) last will, retained: it has to be registered before
    connect() - the broker only learns it from the CONNECT packet - which is
    why it's a parameter here rather than something the caller sets after."""
    kwargs = {}
    if client_id is not None:
        kwargs["client_id"] = client_id
    if clean_session is not None:
        kwargs["clean_session"] = clean_session
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, **kwargs)
    if username:
        client.username_pw_set(username, password or "")
    if use_tls(port, tls):
        # Verified against the system CA store (a public Let's Encrypt cert
        # on Baler's broker), or MQTT_CA_FILE for a private CA.
        client.tls_set(ca_certs=os.environ.get("MQTT_CA_FILE") or None)
    if will is not None:
        client.will_set(will[0], will[1], qos=1, retain=True)

    def _combined_on_connect(client, userdata, flags, reason_code, properties) -> None:
        _on_connect(client, userdata, flags, reason_code, properties)
        if on_connect_extra is not None:
            on_connect_extra(client, userdata, flags, reason_code, properties)

    client.on_connect = _combined_on_connect
    client.connect(host, port)
    client.loop_start()
    return client


def _publish(client: mqtt.Client, topic_prefix: str, point: str, value, ts: float, retain: bool = True,
             priority: dict | None = None) -> str:
    envelope = {"point": point, "value": value, "ts": ts}
    if priority is not None:
        envelope["priority"] = priority
    payload = json.dumps(envelope)
    topic = f"{topic_prefix}/{point}"
    client.publish(topic, payload, qos=1, retain=retain)
    return topic


def publish_reading(
    client: mqtt.Client, topic_prefix: str, point: str, value, ts: float | None = None, retain: bool = True,
    priority: dict | None = None,
) -> str:
    """Publishes {"point", "value", "ts"} to f"{topic_prefix}/{point}",
    retained. This exact shape is the frozen wire contract every consumer
    (Haxall's task engine, Baler's mqtt_listener) parses by name.

    `priority` (Windrow#25) adds an optional key for a commandable point:
    its priority array as {"active": 8, "levels": {"8": 72.0, "16": 68.0}}
    (bacnet_values.priority_levels). Consumers that parse by name ignore
    it.

    If the client isn't currently connected, the reading is spooled to
    disk instead of dropped, and replayed automatically once the
    connection is back (see _on_connect).

    retain=False is for history - a backfilled trend-log record (Baler#129)
    - which must not replace the point's retained current value."""
    ts = ts if ts is not None else time.time()
    if not client.is_connected():
        record = {"topic_prefix": topic_prefix, "point": point, "value": value, "ts": ts}
        if not retain:
            record["retain"] = False
        if priority is not None:
            record["priority"] = priority
        _spool_append(record)
        return f"{topic_prefix}/{point}"
    return _publish(client, topic_prefix, point, value, ts, retain=retain, priority=priority)


def clear_retained(client: mqtt.Client, topic_prefix: str, point: str) -> str:
    """Publishes an empty retained payload to f"{topic_prefix}/{point}",
    which is how MQTT deletes a retained message - without it a point
    removed from a connection (or moved to a new topic_prefix) keeps
    handing its last value and tags to every new subscriber forever.
    Deliberately not spooled like publish_reading: replaying a delete from
    the spool after a newer value could wipe a live reading, while paho
    already queues a QoS 1 publish made while disconnected."""
    topic = f"{topic_prefix}/{point}"
    client.publish(topic, b"", qos=1, retain=True)
    return topic
