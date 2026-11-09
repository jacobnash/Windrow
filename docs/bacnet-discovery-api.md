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

Omit `address` to broadcast Who-Is instead of targeting one device. The
broadcast is global (`*:*`), so routers pass it on to the networks behind
them — see "Other subnets and MS/TP" below for devices that aren't on this
subnet. (A **locally-originated** broadcast doesn't loop back to
another process on the *same* machine over a real physical interface —
that's a macOS/BSD networking limitation, not something this API works
around; broadcast discovery works identically against real devices on a
real network.)

```json
[{"device_instance": 3456, "address": "192.168.128.63", "network": null, "mac": "192.168.128.63",
  "vendor_id": 999, "max_apdu": 1476}]
```

### Other subnets and MS/TP (BBMDs, foreign devices, routers)

A real building has BACnet on several IP subnets, joined by BBMDs, and MS/TP
trunks behind BACnet routers. Windrow reaches all of them the standard ways:

- **Another IP subnet, through a BBMD.** Register as a foreign device with
  the site's BBMD: `--foreign 10.1.0.1 --ttl 300`, or in the environment
  `WINDROW_BACNET_FOREIGN_BBMD=10.1.0.1` and `WINDROW_BACNET_FOREIGN_TTL=300`
  (the command line wins). bacpypes3 renews the registration every TTL and
  retries every 5 s until the BBMD answers; Who-Is broadcasts then go to the
  BBMD, which distributes them to every subnet in its table. Windrow can
  also *be* the BBMD (`--bbmd [peer ...]` / `WINDROW_BACNET_BBMD_BDT`, space-
  or comma-separated peers, empty for none). `GET /bacnet/network` shows the
  mode and whether the BBMD accepted the registration (`registered`,
  `registering`, `unregistered`, `rejected (code)`); the status heartbeat
  carries the same as `bacnet_network`.
- **Behind a router (MS/TP, or a routed IP network).** A device there
  answers the global Who-Is from a routed address, `net:mac`: `5:12` is MAC
  12 on network 5, `7:10.0.1.20` a BACnet/IP device on routed network 7.
  Create the connection with that address as `device_address`, the same as
  any other. bacpypes3 finds the router for the network (from the I-Am, or by
  asking Who-Is-Router-To-Network); after 3 failed reads in a row the cached
  route is dropped and looked up again, so a router that was replaced or
  re-addressed doesn't leave the connection asking the old one forever.
  `{"network": 5}` in the discover body broadcasts on that one network;
  `POST /discover/routers` lists every router and the networks it reaches.
- `{"global_broadcast": false}` keeps the Who-Is to this subnet, as before.

`max_apdu` comes from the device's I-Am; MS/TP devices are usually 480.

`windrow.mock_router` simulates this: a BACnet/IP router + BBMD with mock
devices on a virtual network behind it (`python -m windrow.mock_router
--address 192.168.1.40/24:47808 --bbmd --device 5:4242`). It doesn't
simulate MS/TP itself — token passing, timing and the small APDUs are not
there.

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

### The evidence beyond the name

Real sites have bad names, so learn also reads, in the same
ReadPropertyMultiple, the properties that identify a point when its name
doesn't (Windrow#19). They come back raw; Windrow decides nothing from
them beyond its own deterministic pre-tag.

| Field | From | For |
|---|---|---|
| `object_type` | the identifier | every point |
| `commandable` | `relinquish-default` answered (the standard requires it exactly when there's a priority array) | outputs and values |
| `state_text` | `state-text` | multi-state |
| `active_text`, `inactive_text` | `active-text`, `inactive-text` | binary |
| `min_pres_value`, `max_pres_value`, `cov_increment` | the same-named properties | analog |
| `structured_views` | names of the `structured-view` objects whose `subordinate-list` includes the point | any point an integrator grouped |
| `device` | the device object's `object-name` (`name`), `vendor-name`, `model-name`, `location`, plus `instance` | every point |

A property the object doesn't have is left out rather than `null`.
Structured views aren't returned as points. The device read is best
effort: a device that won't answer it still has its points learned.

```json
{"object_identifier": "analog-value,1", "label": "zone-temp", "present_value": 68.0,
 "units": "degrees-fahrenheit", "description": "Simulated zone temperature",
 "object_type": "analog-value", "commandable": true, "cov_increment": 0.1,
 "device": {"instance": 3456, "name": "mock", "vendor_name": "BACpypes", "model_name": "N/A"}}
```

Pass the learned point to create-a-connection as it came back: these
fields are kept on the stored point and published with its tags
(`{prefix}/{label}/tags`) as `bacnetObjectType`, `bacnetCommandable` (a
marker, only when true), `bacnetStateText`, `bacnetActiveText`,
`bacnetInactiveText`, `bacnetMinPresValue`, `bacnetMaxPresValue`,
`bacnetCovIncrement`, `bacnetStructuredView`, `bacnetDeviceName`,
`bacnetVendorName`, `bacnetModelName` and `bacnetLocation`, beside the
pre-tag's markers. `bacnetObjectType` is published for every point, learned
or not, since the identifier always says it. Baler's classification ladder
reads them as evidence.

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

## 3c. How the device is read: ReadPropertyMultiple and COV

Reads are batched per device with ReadPropertyMultiple by default. Every
point on one device ticks on the same schedule (per interval), and the
reads asked of a device within 20 ms (`WINDROW_RPM_WINDOW`) go out as one
request per `read_multiple_max` objects - across all connections on that
device, since one controller often backs many equipment connections. On
the mock hospital site (492 points, 88 connections, one device) that is
10 requests per poll cycle instead of 491.

- The batch size defaults to what fits the device's max APDU (from its
  I-Am: ~23 for a 480-byte MS/TP device), at most `WINDROW_RPM_MAX_POINTS`
  (50). Set `read_multiple_max` on a connection to go lower.
- A device that rejects RPM (Reject unrecognized-service, a services-class
  Error) is read with single ReadProperty requests from then on. One that
  aborts because the answer doesn't fit gets half-size batches. A timed-out
  RPM fails the batch (the device is down) and the usual back-off applies;
  an answer that names an object nobody asked for is discarded and each
  point re-read on its own.
- `"read_multiple": false` on a connection keeps one ReadProperty per point.

`"subscribe_cov": true` on a connection (or on a point, which overrides the
connection) subscribes each point with SubscribeCOV, so a change publishes
as soon as the device notifies - tens of milliseconds against the mock
device. A subscribed point is still polled every `cov_poll_interval`
(default 300 s) in case a notification is lost. Subscriptions ask for
`cov_lifetime` seconds (default 300) and are renewed at half that; when a
subscribe or renewal fails the point drops back to its normal poll rate and
re-subscribes with back-off, and a device that comes back after an outage is
re-subscribed straight away. An object the device won't do COV for is just
polled. `his_collect_cov` and friends apply the same way whether a value
came from a poll or a notification.

```json
{"device_address": "10.0.4.20", "device_instance": 4020, "topic_prefix": "fbf/ahu-3",
 "poll_interval": 5.0, "subscribe_cov": true, "cov_poll_interval": 120,
 "points": [{"object_identifier": "analog-input,1", "label": "dat"},
            {"object_identifier": "analog-input,7", "label": "oat", "subscribe_cov": false}]}
```

Requests sent so far, by kind, are in the command channel's status heartbeat
as `bacnet_requests` (`read_property`, `read_property_multiple`,
`subscribe_cov`, `cov_notifications`, and the live `cov_subscriptions`).

### Reading quality: status-flags and reliability

BACnet says when a value is bad. Each batched read also asks the object's
`status-flags` and `reliability`, in the same ReadPropertyMultiple, and a
COV notification carries the status-flags with the value. When anything is
set, the reading's envelope gets a `quality` key (Windrow#20):

```json
{"point": "dat", "value": -40.0, "ts": 1762700000.0,
 "quality": {"status_flags": ["fault"], "reliability": "no-sensor"}}
```

BACnet's own names, not a verdict: `in-alarm` alone is reported but Baler
doesn't treat it as bad data. A healthy point's envelope has no `quality`
and is exactly what it was before; an older listener just ignores the key.
A change of quality publishes even when the value didn't move (a sensor
failing at its last value), whatever `his_collect_cov` says.

The extra properties take room in the answer, so a status-reading batch to
a small-APDU device holds fewer objects (~13 for 480 bytes instead of ~23).
Points read with single ReadProperty - `"read_multiple": false`, or a
device without RPM - publish no quality: tripling their requests isn't
worth it. `"read_quality": false` on a connection turns it off.

### Priority arrays: who is holding a point

Every `priority_interval` seconds (default 60, `WINDROW_PRIORITY_INTERVAL`;
`0` turns it off) Windrow reads the `priority-array` of each commandable
point (outputs and values), one ReadPropertyMultiple per 8 points, and the
point's readings carry it (Windrow#25):

```json
{"point": "zone-temp-sp", "value": 75.0, "ts": 1762700000.0,
 "priority": {"active": 8, "levels": {"8": 75.0, "16": 72.0}}}
```

`levels` holds only the levels someone holds; `active` is the lowest of
them, the one in control (`null` when every level is empty and
relinquish-default is). Windrow doesn't know who wrote a level. Baler does
for its own writes (`point_write_levels`), so anything else it sees held is
someone else's: a human override at the panel or another BMS. That's what
Baler's passive override tests (Baler#212) start from.

A change in the array publishes straight away, even when the value didn't
move and `his_collect_cov` would hold it back. A Windrow write
(`write_point`) re-reads the array at once. A device without RPM is asked
point by point; an object that answers it has no priority-array isn't
asked again. Points that aren't commandable, and connections with
`priority_interval: 0`, publish exactly the envelope they did before. Reads
only: nothing is written to the device.

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

## 3e. Schedules and calendars

For commissioning's as-built vs spec check, Windrow reads a device's
Schedule and Calendar objects on demand:

```
curl -X POST localhost:8001/schedules -d '{"device_address": "10.0.4.20", "device_instance": 4020}'
curl localhost:8001/connections/<id>/schedules   # same, with the connection's points named
```

Each schedule comes back with:
- `weekly`: monday..sunday, each a list of `{time, value}`;
- `exceptions`: `{period, priority, times}`, where `period` is a `date` (`*` for any field, e.g.
  `*-12-25`), a `date_range`, a `week_n_day` (`{month, week, weekday}`), or a `calendar`
  reference;
- `effective_period`, `default`, `priority_for_writing`, `present_value`;
- `controls`: the objects and properties it writes, with the connection's `label` where it polls
  that object.

Values are numbers, or `active`/`inactive` when every controlled object is binary. Calendars come
back as their `dates`. An exception-schedule too big for one unsegmented answer is read element by
element; an optional property the device doesn't have comes back null. The command channel has the
same as `read_schedules` and `connection_schedules`.

`python -m windrow.mock_device --schedules` adds `schedule,1` (weekdays 07:00-18:00, a holiday
calendar exception and an early close on the fourth Thursday of November) driving the first binary
object, and `calendar,1`.

## 3f. Alarms (intrinsic reporting)

With `"forward_alarms": true` on a connection, a controller's own alarms -
an analog point past its high/low limit, a binary point in its alarm state,
a fault - reach Baler:

- Windrow adds itself (its address, or its device id for a device behind a
  router) to the recipient-list of every `notification-class` object on the
  device, for all three transitions, as confirmed notifications. This
  changes the controller's configuration, which is why it's opt-in. It
  re-checks every `WINDROW_ALARM_REFRESH` seconds (default 3600), so a
  controller restored from a backup gets the entry back. Turning
  `forward_alarms` off, or deleting the connection, removes the entry.
- Each event notification is published, retained, on
  `{topic_prefix}/{label}/alarm` when the object is one of the connection's
  points, else on `{topic_prefix}/alarms/{type}-{instance}`. The `value` is
  the event: `object`, `event_type`, `from_state`, `to_state`,
  `notify_type`, `priority`, `ack_required`, `message`, `device_timestamp`,
  and `ts` (when Windrow got it).
- When the connection starts (including after a restart), the device's
  active alarms are read with GetEventInformation and published the same
  way, with `source: "summary"` and `event_state`. That way an alarm raised
  while Windrow was down isn't missed.
- `GET /connections/{id}/alarms` (command `connection_alarms`) reads the
  active alarms on demand.

Baler doesn't ingest `/alarm` topics into its alarm console yet; that's a
Baler change. `python -m windrow.mock_device --intrinsic-reporting` makes
the mock's analog objects alarm 10 either side of their start value, through
notification-class,1. Write present-value past a limit at priority 8 to
raise an alarm, and relinquish it to clear the alarm.

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
