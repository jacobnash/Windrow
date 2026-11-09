# Remote command channel

Design background: `docs-site/pages/multi-site.mdx` in the baler
repo. `windrow.api` already has a real HTTP surface for discovery/scanning
(`POST /discover`, `POST /modbus/discover`, `POST /learn`), but a central
Baler deployment typically has no inbound path into a building's
network. This reuses the same outbound MQTT connection `windrow.api` already
holds open to publish readings, instead of opening a new port.

## Enabling it

Off by default - a bare `windrow.api` run behaves exactly as before, no
command topic, no persistent session:

```
python -m windrow.api --address 192.168.128.63/24:47820 --mqtt-host localhost \
  --mqtt-username <site-username> --mqtt-password <site-password> \
  --site-id hospital-a --instance-id ahu-bridge-1
```

- `--site-id` (or `WINDROW_SITE_ID`) turns the feature on. Unset, nothing below
  applies.
- `--instance-id` (or `WINDROW_INSTANCE_ID`) defaults to the host's hostname -
  set it explicitly if a site runs more than one `windrow.api` process.
- `--mqtt-username`/`--mqtt-password` (or `MQTT_USERNAME`/`MQTT_PASSWORD`)
  authenticate to mosquitto. Once a site has a command channel, mosquitto's
  ACL scopes that username to its own `cmd/<username>/#` - see
  baler's `mosquitto/acl`. **The username must equal `--site-id`**
  for that scoping to actually restrict anything.

## Sending a command

Publish a JSON `{"action": ..., ...}` message to
`cmd/<site-id>/<instance-id>/#` (any suffix after the instance id works -
`windrow.api` subscribes to the whole subtree):

```
mosquitto_pub -h <broker> -u hospital-a -P <site-password> \
  -t cmd/hospital-a/ahu-bridge-1/modbus-discover \
  -m '{"action": "modbus_discover", "cidr": "10.2.0.0/24"}'
```

Supported `action` values, each dispatching to the same code its HTTP
equivalent calls (the `handle_*` functions in `src/windrow/api.py`):

| action | same as | body fields |
| --- | --- | --- |
| `discover` | `POST /discover` | passed through as kwargs (`address`, `network`, `global_broadcast`, `timeout`, `low_limit`/`high_limit`) |
| `discover_routers` | `POST /discover/routers` | optional `network`, `timeout` |
| `network_status` | `GET /bacnet/network` | none |
| `learn` | `POST /learn` | `device_address`, `device_instance` |
| `read_schedules` | `POST /schedules` | `device_address`, `device_instance` |
| `connection_schedules` | `GET /connections/{id}/schedules` | `id` |
| `modbus_discover` | `POST /modbus/discover` | `cidr`, optional `port`/`concurrency`/`timeout` |
| `point_write` | `POST /write` | `topic`, `value` (`null` relinquishes), `priority` 1-17 (17 sets BACnet relinquish-default; it cannot be relinquished), optional `expires_at` (unix s). A topic a Modbus connection publishes goes to its writable holding register or coil - see below |
| `write_schedule` | (command channel only) | `id` (connection), `object` (`schedule,N`), any of `weekly`/`exceptions`/`default`/`effective_period`, optional `expires_at`, `who`, `reason` - see [Schedule and calendar writes](#schedule-and-calendar-writes) |
| `write_calendar` | (command channel only) | `id` (connection), `object` (`calendar,N`), `dates`, optional `expires_at`, `who`, `reason` |
| `list_devices` | `GET /devices` | none |
| `list_announced` | `GET /announced` | none |
| `add_device` | `POST /devices` | `protocol: "bacnet"`, `device_address`, `device_instance` - or `protocol: "modbus"`, `host`, optional `port` (502) / `device_id` (1) |
| `set_device_status` | `PATCH /devices/{id}` | `id`, `status` (`pending`/`provisioned`/`ignored`) |
| `recheck_credentials` | `POST /devices/{id}/recheck-credentials` | `id` |
| `list_connections` | `GET /connections` | none |
| `connection_alarms` | `GET /connections/{id}/alarms` | `id` |
| `create_connection` | `POST /connections` | as the HTTP body: `device_address`, `device_instance`, `topic_prefix`, `points`, optional `poll_interval`/`credential` |
| `update_connection` | `PATCH /connections/{id}` | `id`, plus any of `points`, `poll_interval`, `topic_prefix` |
| `delete_connection` | `DELETE /connections/{id}` | `id` |
| `modbus_list_connections` | `GET /modbus/connections` | none |
| `modbus_create_connection` | `POST /modbus/connections` | as the HTTP body: `host`, `topic_prefix`, `points` (`{label: register or point object}`), optional `port`/`device_id`/`poll_interval`/`units`/`credential` |
| `modbus_update_connection` | `PATCH /modbus/connections/{id}` | `id`, plus any of `points`, `units`, `poll_interval`, `topic_prefix`, `host`, `port`, `device_id` |
| `modbus_delete_connection` | `DELETE /modbus/connections/{id}` | `id` |
| `start_job` | `POST /jobs` | `job` (`discover`, `learn` or `modbus_discover`) plus that action's fields. Answers at once with the job; use it for anything that can take more than a few seconds |
| `get_job` | `GET /jobs/{id}` | `id` (the job's): `state` (`running`/`done`/`failed`), `progress` (`{done, total}` for a sweep), `result` once done, `error` if failed |
| `list_jobs` | `GET /jobs` | none |
| `modbus_test_read` | `POST /modbus/test-read` | `host`, `register`, optional `port` (502) / `device_id` (1). Reads once; stores and publishes nothing |
| `set_credential` | `PATCH .../{id}/credential` | `target` (`device`, `connection` or `modbus_connection`), `id`, `credential: {username, password}` |

The outcome (`{"ok": true, "result": ...}` or `{"ok": false, "error": ...}`,
plus the `action`, and the command's own `request_id` and/or `id` if it sent
them) is published to `cmd/<site-id>/<instance-id>/result` - a command is
observable, not fire-and-forget. Send a `request_id` when more than one caller
may be waiting on results (older callers correlate on `id`, which is still
echoed). An unknown (or missing) `action` gets an `{"ok": false, "error":
"unknown action ..."}` result too; only a payload that isn't a JSON object
is dropped without one.

The envelope fields - `action`, `id`, `request_id` and `expires_at` - are
stripped before the action runs, for every action, so they never reach it as
parameters. The exceptions are the ones that are parameters: a targeted
action's `id` (below), and `point_write`'s `expires_at` (a write that arrives
after it is refused, not applied late; the same for `write_schedule` and
`write_calendar`). `discover` takes only `low_limit`,
`high_limit`, `address` and `timeout`; other fields are ignored.

For the actions that act on one device or connection (`set_device_status`,
`recheck_credentials`, `update_connection`, `delete_connection`, their
`modbus_*` twins, `set_credential`, `get_job`, `write_schedule` and
`write_calendar`), `id` is the id of that device or connection, so correlate
those with `request_id`.

Results are what the HTTP route returns: device and connection records
with `credential` redacted to `{"username": ..., "password": null}`, exactly
as every HTTP GET shows them; a "not found" is `{"ok": false, "error": "no
connection with that id"}` (or `"no device with that id"`);
`delete_connection`/`modbus_delete_connection` return `{"id": ..., "deleted":
true}`. One deliberate difference: `set_credential`'s result is the redacted
record, not the plaintext echo `PATCH .../credential` answers with - a
result topic can be read by anything subscribed to it, and the sender
already knows the password.

### Modbus points and writes

A Modbus point is a bare holding-register address (read as one unsigned
16-bit word - the original form, still valid) or an object:

```json
{"register": 4, "function": "holding", "type": "int16", "word_order": "big",
 "byte_order": "big", "scale": 0.1, "offset": 0, "unit": "°F", "writable": true}
```

`function` is `holding` (default), `input`, `coil` or `discrete`; `type` is
`int16`, `uint16` (default), `int32`, `uint32` or `float32`; `word_order`
(32-bit types) and `byte_order` are `big` (default) or `little`. The
published value is `raw * scale + offset`; coils and discrete inputs publish
`true`/`false`. `register` is the 0-based protocol address: holding register
40001 is `register: 0`, input register 30010 is `function: input,
register: 9`. Every field is checked strictly - an unknown key or an option
that doesn't apply (scale on a coil, `writable` on an input register) is
refused. `src/windrow/modbus_points.py` has the details; the mock device's
`POINTS` (`src/windrow/mock_modbus_device.py`) is a worked example of each.

A `writable` point is tagged `writable` (so Baler accepts a pointWrite) and
takes `point_write`, with the same `priority` and `expires_at` rules as
BACnet. A value is in engineering units and is encoded back through the
point's scale, offset and type. Modbus devices have no priority array, so
Windrow keeps one per point, stored on the connection as `write_levels`, and
writes the highest-priority level's value; a `null` relinquishes a level and
the next one down is written. With every level relinquished nothing is
written and the register keeps its last value.

`update_connection` restarts that connection's polling with the new
settings, republishes its point tags and `{prefix}/equip/tags`, and sends an
empty retained message to `{old_prefix}/{label}` and `{old_prefix}/{label}/tags`
for every point that no longer publishes there (a removed label, or all of
them when `topic_prefix` changes), so Baler stops being handed their last
value. `points` replaces the whole list (BACnet) or map (Modbus).

```
mosquitto_pub -h <broker> -u hospital-a -P <site-password> \
  -t cmd/hospital-a/ahu-bridge-1/manage \
  -m '{"action": "update_connection", "id": "419dff013a6d4d7783b43e6603ccefd7", "poll_interval": 10}'
```

publishes, on `cmd/hospital-a/ahu-bridge-1/result`:

```json
{"ok": true, "action": "update_connection", "id": "419dff013a6d4d7783b43e6603ccefd7",
 "result": {"id": "419dff013a6d4d7783b43e6603ccefd7", "device_address": "192.168.128.63:47808",
            "device_instance": 3456, "topic_prefix": "fbf/hospital-a/ahu-1", "poll_interval": 10,
            "points": [{"object_identifier": "analog-value,1", "label": "zone-temp"}],
            "credential": null}}
```

### Schedule and calendar writes

`write_schedule` and `write_calendar` change a BACnet Schedule or Calendar
object on a connection's device (Windrow#26). They are the write side of
`connection_schedules`: the same shapes, so what Baler reads it can change
and send back, and its revert is the old object written back the same way.
Baler's audited schedule writes (Baler#213) are the sender. Baler keeps the
audit (who, why, the before-state, when it reverts), and Windrow applies the
write.

```json
{"action": "write_schedule", "id": "<connection id>", "request_id": "…", "expires_at": 1762700030.0,
 "who": "op:7f3c…", "reason": "Holiday hours for the 24th",
 "object": "schedule,1",
 "weekly": {"monday": [{"time": "06:30", "value": "active"}, {"time": "20:00", "value": "inactive"}],
            "tuesday": […], "wednesday": […], "thursday": […], "friday": […], "saturday": [], "sunday": []},
 "exceptions": [{"period": {"date": "2026-12-24"}, "priority": 3,
                 "times": [{"time": "08:00", "value": "active"}, {"time": "12:00", "value": "inactive"}]}],
 "default": "inactive",
 "effective_period": ["2026-01-01", "2026-12-31"]}
```

```json
{"action": "write_calendar", "id": "<connection id>", "request_id": "…", "expires_at": 1762700030.0,
 "object": "calendar,1", "dates": [{"date": "2026-12-25"}, {"date_range": ["2026-12-28", "2026-12-31"]},
                                    {"week_n_day": {"month": 11, "week": 4, "weekday": "thursday"}}]}
```

- **Fields:** keys left out aren't written. `weekly` has all seven days,
  each up to 24 `{time, value}` entries. `exceptions` replaces the whole
  exception schedule (up to 64), each `{period, priority (1-16, default 16),
  times}`, with a period of `{date}`, `{date_range}`, `{week_n_day}` or
  `{calendar: "calendar,N"}`. A calendar's `dates` (up to 128) take the same
  periods except `calendar`. Dates are `YYYY-MM-DD` with `*` for any field
  (and `odd`/`even` months, `last`/`odd`/`even` days); times are
  `HH:MM[:SS[.hh]]`; `null` as a value relinquishes.
- **Values** are encoded for what the schedule drives: `active`/`inactive`
  for binary objects, a state number for multi-state, a number for analog.
  With no controlled objects, the type follows the schedule's current default.
- **Safeguards** (the same as `point_write`): refused after `expires_at`.
  The object must exist: it's read first, which also tells Windrow how to
  encode values. Every field is validated before anything is sent, so a bad
  command writes nothing. Then one bounded WriteProperty per field, in the
  order `effective_period`, `weekly`, `exceptions`, `default`. If the device
  refuses one, the error names it and lists any fields already written
  (`"… refused writing schedule,1 exception-schedule: …; already written:
  effective-period, weekly-schedule"`). `who` and `reason` are optional and
  only logged by Windrow; Baler keeps the audit record.
- **Auth:** like every action, the sender needs the site's MQTT credentials
  and mosquitto's ACL. There's deliberately no HTTP route.
- **Result:** the object read back after the write, in
  `connection_schedules`' format (one schedule or calendar, `controls`
  labelled), so the caller sees what the device actually holds.
- **Segmentation:** a long exception schedule may not fit one APDU on a
  small controller. The device then refuses the write and nothing past
  that field is written.

## Status heartbeat

With the command channel on, `windrow.api` keeps a **retained** JSON message
on `cmd/<site-id>/<instance-id>/status`, published at startup, on every
(re)connect, and every 60 s (`STATUS_INTERVAL` in `api.py`):

```json
{"online": true, "site_id": "hospital-a", "instance_id": "ahu-bridge-1",
 "version": "0.1.0", "started_at": "2026-11-07T09:00:00.123456+00:00",
 "ts": "2026-11-07T09:05:00.654321+00:00",
 "bacnet_connections": 3, "modbus_connections": 1, "devices": 9, "pending_devices": 2,
 "bacnet_network": {"mode": "foreign", "bbmd": "10.1.0.1", "registration": "registered"},
 "bacnet_requests": {"read_property_multiple": 1200, "subscribe_cov": 40,
                     "cov_notifications": 310, "cov_subscriptions": 20},
 "spool": {"rows": 0, "capacity": 95040, "dropped": 0},
 "networks": ["192.168.1.0/24"]}
```

`bacnet_network` is how this instance reaches BACnet: `mode` `normal`,
`foreign` (with the BBMD and whether it accepted the registration) or `bbmd`.
`bacnet_requests` counts BACnet requests sent since startup, by kind, plus
the number of live COV subscriptions - see bacnet-discovery-api.md 3c.

`networks` lists the subnets the node sits on (its BACnet `--address` and any
`--modbus-scan-cidr`); Baler's Connectors page prefills its Modbus sweep with
the first one. `GET /status` returns the same payload over HTTP, for a node
reached that way.

Each listed connection (`list_connections`, `modbus_list_connections`, and
their HTTP `GET`s) carries a `poll_status` too: `state` (`waiting`, `ok`,
`partial` or `error`), `last_poll_at`, `last_success_at`, `last_value_at`,
`last_error`, `last_error_at` (unix seconds), and the labels of the points
whose last read failed. It's in memory only and resets when the connection
is edited.

`version` is the installed `windrow` package version (`"dev"` if it isn't
installed). The same topic is the MQTT client's last will, also retained:
`{"online": false, "site_id": ..., "instance_id": ...}` - so if the edge
drops off the network the broker flips it to offline itself. A clean
shutdown (Ctrl-C, or SIGTERM from `docker stop`) publishes that offline
message too. Because it's retained, a subscriber sees each instance's last
state straight away; `ts` going stale while `online` is still true means
the broker hasn't noticed a dead connection yet (it will within 1.5x the
keepalive).

```
mosquitto_sub -h <broker> -u hospital-a -P <site-password> -t 'cmd/hospital-a/+/status' -v
```

`windrow.api` ignores its own `.../result` and `.../status` messages, which
its `cmd/<site-id>/<instance-id>/#` subscription would otherwise hand back
to it as commands.

### Point writes

`point_write` is BACnet WriteProperty on the present-value of the point
that publishes `topic`, at the given priority, exactly like a controller
operator's command: a write at 8 overrides the device's own logic at 16
until it's relinquished (`"value": null` at the same priority). Only
commandable object types (analog/binary/multi-state *output* and *value*)
accept writes; Windrow tags those points `writable` when a connection is
created. After a write, the read-back value is published straight away.

```
mosquitto_pub -h <broker> -u hospital-a -P <site-password> \
  -t cmd/hospital-a/ahu-bridge-1/point-write \
  -m '{"action": "point_write", "id": "w-1", "topic": "fbf/hospital-a/ahu-1/zone-sp", "value": 72, "priority": 8, "expires_at": 1767225600}'
```

Set `expires_at`. The session is persistent, so a command sent while the
site is offline is delivered when it reconnects. For a setpoint, "apply it
an hour late" is worse than "don't apply it", and an expired command is
refused, never applied.

With `--site-id` set, each connection's equipment tags (`{prefix}/equip/tags`)
carry `windrowSite`/`windrowInstance`, which is how Baler knows which
`cmd/<site>/<instance>` topic owns a point it wants to write.

## Why the persistent session matters

`--site-id` also switches the MQTT client to a stable `client_id` and
`clean_session=False`. Without that, a command sent while the site is
offline is dropped, not queued - `clean_session=True` (paho's default)
throws away the broker-side session, and everything queued for it, on
every disconnect. With it, mosquitto's `max_queued_messages` (see
baler's `mosquitto/mosquitto.conf`) holds commands until this
process reconnects.
