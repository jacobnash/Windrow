# Adding a new source

Windrow's whole design is that "port Haxall's connector breadth" is a
repeatable recipe, not one-off work per protocol. This is that recipe.

## The wire contract (frozen, don't redesign this without a strong reason)

- **Topic**: `f"{topic_prefix}/{point}"` — e.g. `fbf/mock-ahu-1/analogValue,1`
  or `fbf/mock-modbus-1/zone-temp`. `point` is whatever label makes sense
  for the protocol (a BACnet object identifier, a Modbus register label,
  anything stable and human-legible).
- **Payload** (JSON): `{"point": <str>, "value": <int|float|str|bool|null>, "ts": <unix epoch float>}`.
  One optional key may follow: `"quality"`, only when the device says
  something is wrong with the value (Windrow#20). For BACnet it's the
  object's own words, `{"status_flags": ["in-alarm"|"fault"|"overridden"|
  "out-of-service", ...], "reliability": "<BACnet reliability>"}`, either
  key absent when it has nothing to say. Consumers that parse by name
  ignore it; Baler stores it as the sample's quality flag
  (`baler.quality.from_envelope`). No `quality` means nothing reported.
  Pass it as `publish_reading(..., quality=...)`, never build it into
  `value`.
- **`retain=True`** on every publish — a fresh subscriber immediately gets
  the last known value per point instead of waiting for the next poll.
- Every bridge imports `windrow.mqtt_sink` (`connect()`, `publish_reading()`)
  for this — never re-derive the envelope/publish logic per bridge.

Nothing that consumes this cares which protocol produced it. That's what
makes this a zero-coordination-cost pattern: adding a new source never
requires touching a central registry, config, or any other bridge's code.

### The `/tags` suffix convention (metadata, not telemetry)

Some sources carry more than a bare value — Haystack markers, equipment
refs, units. That's static, once-per-point metadata, not high-frequency
telemetry, so it doesn't belong in the hot-path envelope above. Instead,
publish it through the *same* `publish_reading()` helper, once per point at
startup, under a topic ending in `/tags`:

```python
mqtt_sink.publish_reading(client, topic_prefix, f"{point_label}/tags", json.dumps(tag_dict))
```

This is still a valid instance of the frozen envelope — `value` is just a
JSON string instead of a scalar. On the consumer side, Baler's
`mqtt_listener.on_message` checks for the `/tags` suffix, strips it to
recover the real point's topic (so it resolves to the *same* `point_uri`
the value-topic would produce), and routes to `ingest_tags()` instead of
`ingest_reading()`. See `haystack_source.py`'s `extract_tags()` /
`read_once()` for the producing side — this is the only source that emits
`/tags` today, but any future source with real metadata to carry can reuse

**Export generically, not off a hand-picked list.** `extract_tags()` uses a
short deny-list (structural/live-state fields like `id`, `mod`, `curVal`)
plus `to_jsonable()`, a converter covering every real Haystack v3/v4 value
kind (Marker, Ref, Uri, Coord, XStr, Date/Time/DateTime, NA, Remove, nested
Dict/List — see the [Kinds chapter](https://github.com/Project-Haystack/haystack-defs/blob/master/src/docHaystack/doc/Kinds.fandoc)
for the authoritative list), falling back to `str()` for anything it
doesn't recognize. An earlier version allow-listed 7 markers it happened to
think of — that silently dropped every other real tag (including `unit`,
which matters) on any building that wasn't this exact demo. Allow-lists of
"tags we thought of" don't survive contact with real Haystack data; a
deny-list of the few fields that are genuinely wrong to republish does.
the same convention with zero changes to the listener.

## The recipe

1. Find a mature, permissively-licensed open-source client library for the
   protocol. Don't write a protocol implementation from scratch — that's
   the whole reason BACnet (BACpypes3) and Modbus (pymodbus) bridges took
   an evening each instead of weeks.
2. Write `<protocol>_bridge.py` on the same skeleton as `bridge.py` /
   `modbus_bridge.py`: an argparse CLI, a poll loop (or event
   subscription, if the protocol is push-based), a `normalize_value()`
   function *only if* the library's native types aren't already
   JSON-safe (BACnet's `Enumerated` needed one; Modbus's plain ints
   didn't).
3. Call `mqtt_sink.publish_reading()` for each reading. Done — nothing
   else to register anywhere.
4. Write a `mock_<protocol>_device.py` if the protocol's own ecosystem
   ships a simulator/test server (BACpypes3 and pymodbus both do) — lets
   the bridge be tested without real hardware.

## Candidate table: Haxall's connector list → likely client library

Haxall (an open building-automation framework) ships open connectors for
several protocols/APIs. Their Fantom implementations aren't portable here
(no Fantom, by design) — but the *list* is a useful backlog of what to
bridge next, each following the recipe above:

| Protocol/API | Likely client library | Notes |
|---|---|---|
| Modbus | `pymodbus` | Done — see `modbus_bridge.py` for the static CLI bridge (still the simplest path if you already know the device's register addresses), or `modbus_discovery.py` + `modbus_connection_manager.py` (exposed via `windrow.api`'s `/modbus/*` routes, see `openapi.yaml`) for the CIDR-sweep→create-connection API when you don't. Modbus has no self-describing object list like BACnet's, so there's no `/modbus/learn` - a human supplies `{label: register_address}` points directly. |
| BACnet | `bacpypes3` | Done — see `bridge.py` for the static CLI bridge (still the simplest path if you already know the device's object identifiers), or `docs/bacnet-discovery-api.md` for the discover→learn→create-connection API when you don't. `normalize_value()` now lives in `bacnet_values.py`, shared by both. |
| SQL | `psycopg` (Postgres) or similar | Poll rows, map columns to points |
| oBIX | raw XML+HTTP, or an `obix` client if one's actively maintained | Check for one before hand-rolling |
| Ecobee / Nest | their REST APIs via `requests` + OAuth | Standard OAuth flow, not protocol-novel |
| Haystack HTTP API (nhaystack, SkySpark, Haxall) | `openhay` | Done — see `haystack_source.py` and [haystack-source.md](haystack-source.md). openhay handles SCRAM (including clients' and servers' base64 quirks), Zinc / JSON v3 / Hayson, watches and pointWrite, so none of the old pyhaystack landmines apply. The Haxall demo tooling (`haxall_client.py`) still uses pyhaystack; its landmines are in `docs/haxall-mqtt-integration.md`. |

Native MQTT devices need **no bridge at all** — they already speak the
transport. If their payload doesn't already match the envelope above, a
tiny field-mapping shim (not a full bridge) is enough.

## Periodic discovery, the device registry, and credentials

`windrow.api` runs a periodic BACnet Who-Is scan by default
(`--bacnet-discovery-interval`, 300s), and one periodic Modbus CIDR sweep
per `--modbus-scan-cidr` flag if any are configured (off by default - no
safe subnet to guess). Both feed `device_registry.DeviceRegistry`, which
tracks every device either scan has ever seen (`GET /devices`) whether or
not a human has provisioned a connection for it yet - separate from
`connections.json`/`windrow-modbus-connections.json`, since a bare discovery
result has no `id`/`topic_prefix`/`points` and those files' own invariant
("every record here is actively being polled") shouldn't have to make room
for records that aren't.

Credentials (`credentials.py`, Fernet-encrypted, keyed by
`WINDROW_CREDENTIAL_KEY`) are pure bookkeeping - neither BACnet nor Modbus has
protocol-level auth, so this only ever records a login a human needs for
whatever the device's own admin surface is (a gateway's web UI, etc.),
never wired into a real protocol handshake. `default_credential_check.py`
runs once, automatically, the first time a *Modbus* device is seen as
pending (never BACnet - no HTTP-login concept applies to a raw BACnet
device) - a short hardcoded list of common defaults tried as HTTP Basic
Auth against the device's own address.

Full worked-example recipe (curl-by-curl, same style as
`docs/bacnet-discovery-api.md`): `docs/periodic-discovery-and-credentials.md`.
