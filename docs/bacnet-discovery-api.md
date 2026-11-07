# BACnet discovery + connections API

The "SkySpark-easy, but via API" recipe: discover a device, learn its
points, create a connection with the ones you want — no hand-typed BACnet
object identifiers anywhere in the flow.

```
python -m windrow.api --address 192.168.128.63/24:47820 --mqtt-host localhost
```

(`--address` is bacpypes3's own local-BACnet-address arg, same as
`bridge.py`/`mock_device.py` — pick a port not already in use on this
host.)

**Access.** The API has no logins of its own. Behind Baler, set
`WINDROW_GATEWAY_SECRET` to Baler's `BALER_GATEWAY_SECRET`: every request
must then carry the `X-Gateway-Secret` the gateway adds after checking the
caller's login and role (Baler's `/windrow/` route — reads for any role,
changes admin-only), and direct requests to the port get 401.
`/openapi.yaml` stays public. Unset, the API is open, which is meant for
local development only. No CORS headers are sent unless
`WINDROW_CORS_ORIGIN` names the one origin allowed (e.g.
`http://localhost:8004` for Baler's standalone `ui_server.py`).

## 1. Discover devices

```
curl -X POST localhost:8001/discover -d '{"address": "192.168.128.63:47808", "timeout": 5.0}'
```

Omit `address` to broadcast Who-Is across the whole local network instead
of targeting one device — the exact same code path, just without a
destination. (A **locally-originated** broadcast doesn't loop back to
another process on the *same* machine over a real physical interface —
that's a macOS/BSD networking limitation, not something this API works
around; broadcast discovery works identically against real devices on a
real network.)

```json
[{"device_instance": 3456, "address": "192.168.128.63", "vendor_id": 999}]
```

## 2. Learn a device's points

```
curl -X POST localhost:8001/learn -d '{"device_address": "192.168.128.63:47808", "device_instance": 3456}'
```

```json
[
  {"object_identifier": "network-port,1", "label": "NetworkPort-1", "present_value": null, "units": null},
  {"object_identifier": "analog-value,1", "label": "zone-temp", "present_value": 71.0, "units": "degrees-fahrenheit"},
  {"object_identifier": "binary-value,1", "label": "fan-status", "present_value": "active", "units": null}
]
```

Every object on the device comes back, points included — a `null`
`present_value` (like `network-port,1` above) is a hint it's not a
sensor/actuator point, not an error. `units: null` on `binary-value,1`
isn't a bug either: that object genuinely has no `units` property, and a
per-property read failure never hides the rest of the point's fields (see
`discovery.py`'s `ErrorType` handling).

## 3. Create a connection

Pick whichever learned points you want, optionally renaming `label` (it's
just the MQTT topic segment — matches the wire contract's "point is
whatever label makes sense" convention, same as every other bridge):

```
curl -X POST localhost:8001/connections -d '{
  "device_address": "192.168.128.63:47808",
  "device_instance": 3456,
  "topic_prefix": "fbf/ahu-3",
  "poll_interval": 5.0,
  "points": [
    {"object_identifier": "analog-value,1", "label": "zone-temp"},
    {"object_identifier": "binary-value,1", "label": "fan-status"}
  ]
}'
```

Returns the created record (`201`, includes a generated `id`) and starts
polling immediately — `fbf/ahu-3/zone-temp` and `fbf/ahu-3/fan-status`
start publishing on the configured interval, same envelope every bridge
uses.

## 3b. Per-point sampling rate and change-of-value filtering

Each point in `points` can override the connection's `poll_interval` and opt
into Haystack-compatible history-collection filtering — matching real
`hisCollectCov`/`hisCollectCovRateLimit`/`hisCollectInterval` semantics
(verified against a real SkySpark distribution, not guessed), so a fast
damper position and a slow outside-air-temp sensor on the same device don't
have to share a cadence, and a point that never changes doesn't publish a
redundant reading every poll forever:

```
curl -X POST localhost:8001/connections -d '{
  "device_address": "192.168.128.63:47808",
  "device_instance": 3456,
  "topic_prefix": "fbf/ahu-3",
  "poll_interval": 5.0,
  "points": [
    {
      "object_identifier": "analog-value,1",
      "label": "zone-temp",
      "poll_interval": 1.0,
      "his_collect_cov": 0.5,
      "his_collect_cov_rate_limit": 5.0
    },
    {
      "object_identifier": "analog-value,2",
      "label": "outside-air-temp",
      "his_collect_interval": 300.0
    },
    {
      "object_identifier": "binary-value,1",
      "label": "fan-status"
    }
  ]
}'
```

- `zone-temp` is read every 1s, but only published when it moves by ≥0.5°,
  and never more often than every 5s even if it's actively swinging.
- `outside-air-temp` is read on the connection's default 5s cadence, but
  only published every 5 minutes regardless of whether it changed.
- `fan-status` has no history-collection config — every poll publishes
  unconditionally, the same default behavior as before this feature existed.

Omitting `his_collect_cov`/`his_collect_interval` entirely (as `fan-status`
does) is the safe, backward-compatible default — nothing changes for a
point that doesn't opt in.

## 3d. Backfilling gaps from the controller's trend logs

A controller keeps logging while Windrow is down or can't reach it. Give a
point the TrendLog that logs it, and Windrow reads the gap back:

```json
{"object_identifier": "analog-input,1", "label": "zone-temp", "trend_log": "trend-log,1"}
```

- **After a restart**, the point's first good read backfills from the last
  reading Windrow published for it before going down. That time is kept in
  `<state file>.trend-marks.json` (e.g. `windrow-connections.trend-marks.json`),
  written once a minute and on a clean shutdown. A point with no mark yet
  isn't backfilled.
- **After an outage** (reads failing, then succeeding), the first good read
  backfills from the last good read before the failures.
- Records are read with ReadRange by time, 20 per request, and published on
  the point's own topic with the record's timestamp, **not retained**, so
  the point's retained message stays its live value. Baler stores them as
  history (its ingest is idempotent on point + timestamp, so an overlap
  is harmless). Binary points come back as `active`/`inactive`, like a poll.
- A backfill goes back at most `backfill_window` seconds (per connection,
  default 86400; `0` turns it off). A failed backfill is logged and the live
  poll carries on.
- Log timestamps are the device's local time. Windrow reads the device's
  `local-date`/`local-time` to work out its UTC offset (to the nearest 15
  minutes); a device that doesn't report them is assumed to be on this
  host's time zone. A backfill across a DST change is an hour off on the far
  side of it.

`python -m windrow.mock_device --trend-log-interval 5` adds a TrendLog
(`trend-log,1..N`, in object order) for each mock object, served over
ReadRange.

## 4. List / delete

```
curl localhost:8001/connections
curl -X DELETE localhost:8001/connections/<id>
```

Deleting stops the poll task and removes it from the persisted
`connections.json` — restarting `windrow.api` with connections left in place
resumes polling them with zero new API calls.

## Known v1 limitations

- **No COV rate-limit tiering.** Real Haystack's `hisCollectCov` falls back
  to a tiered default rate limit (1/10 of `hisCollectInterval` or 1min,
  else 1min for numerics / 1sec for non-numerics) when
  `hisCollectCovRateLimit` isn't set. This implementation has no default
  limit at all unless you set one explicitly — simpler, deliberately, add
  the tiering if an unthrottled noisy point turns out to be a real problem.
- **No re-learn-on-change.** If a device's object list changes after a
  connection exists, nothing notices until a human re-learns and
  recreates the connection. Matches SkySpark's own model — discovery is
  user-triggered, not continuously reconciled.
- **No file locking** on `connections.json` — one `windrow.api` process per
  `--state-file` is an assumption, not enforced.

## Real bugs this was verified against, not just assumed to work

Every one of these was found by actually running discovery against the
live `mock_device.py`, not by reading bacpypes3's docs:

- `who_is(address=...)` needs a `bacpypes3.pdu.Address` object, not a raw
  string — a raw string fails deep inside with a confusing
  `AttributeError: 'str' object has no attribute 'is_localstation'`.
- A device's `object-list` property returns plain `(ObjectType, instance)`
  tuples, not `ObjectIdentifier` instances — `read_property_multiple`
  requires the latter (`TypeError("objid")` otherwise).
- `ObjectType` doesn't compare equal to a plain string
  (`obj_id[0] == "device"` is always `False`) — `str()` it first.
- `read_property_multiple`'s `parameter_list` is a **flat** list
  (`[objid1, props1, objid2, props2, ...]`), not a list of `(objid, props)`
  pairs, despite what the type hint implies — it unpacks two elements at a
  time internally.
