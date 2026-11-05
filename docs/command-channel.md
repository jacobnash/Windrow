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

The outcome (`{"ok": true, "result": ...}` or `{"ok": false, "error": ...}`)
is published to `cmd/<site-id>/<instance-id>/result` - a command is
observable, not fire-and-forget.

## Why the persistent session matters

`--site-id` also switches the MQTT client to a stable `client_id` and
`clean_session=False`. Without that, a command sent while the site is
offline is dropped, not queued - `clean_session=True` (paho's default)
throws away the broker-side session, and everything queued for it, on
every disconnect. With it, mosquitto's `max_queued_messages` (see
baler's `mosquitto/mosquitto.conf`) holds commands until this
process reconnects.
