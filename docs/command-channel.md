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
equivalent calls (see `src/windrow/api.py`'s `handle_discover`/`handle_learn`/
`handle_modbus_discover`):

| action | same as | body fields |
| --- | --- | --- |
| `discover` | `POST /discover` | passed through as kwargs |
| `learn` | `POST /learn` | `device_address`, `device_instance` |
| `modbus_discover` | `POST /modbus/discover` | `cidr`, optional `port`/`concurrency`/`timeout` |
| `point_write` | `POST /write` | `topic`, `value` (`null` relinquishes), `priority` 1-17 (17 sets BACnet relinquish-default; it cannot be relinquished), optional `expires_at` (unix s) |

The outcome (`{"ok": true, "result": ...}` or `{"ok": false, "error": ...}`,
plus the `action`, and the command's own `id` if it sent one) is published to
`cmd/<site-id>/<instance-id>/result` - a command is observable, not
fire-and-forget. Send an `id` when more than one caller may be waiting on
results.

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
