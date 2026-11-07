# Edge nodes on a Raspberry Pi

`deploy/pi/install-edge.sh` turns a fresh Raspberry Pi (Raspberry Pi OS Lite
64-bit, or any Debian with systemd) into a Windrow edge node for Baler, one
role per run. It installs Windrow from GitHub into `/opt/windrow`, runs it as
the `windrow` system user, and enables a `windrow-<role>` systemd unit.

```bash
curl -fsSLO https://raw.githubusercontent.com/jacobnash/Windrow/main/deploy/pi/install-edge.sh
sudo bash install-edge.sh bacnet     # or: modbus, weather, gateway
```

| Role | Runs | Listens / sends |
|---|---|---|
| `bacnet` | `windrow.mock_device`, a simulated BACnet/IP device | UDP 47808 on the Pi's LAN address (its /24, so it hears Who-Is broadcasts); instance = last octet + 10000 unless `BACNET_INSTANCE` is set |
| `modbus` | `windrow.mock_modbus_device`, a simulated Modbus TCP server | TCP 502 (via `AmbientCapabilities`, no root, no `setcap`) |
| `weather` | `windrow.mock_weather`, a simulated weather station | publishes to Baler's MQTT broker |
| `gateway` | `windrow.api`, Windrow itself | discovers and polls BACnet/Modbus devices on this LAN, publishes to Baler; UDP 47808 on the Pi's LAN address, HTTP API on 127.0.0.1:8001 |

`bacnet` and `gateway` can't share a Pi: both bind UDP 47808 on its address
(Windrow has to, since bacpypes3 sends Who-Is to its own port). The script
refuses the second one; `sudo systemctl disable --now windrow-<role>` first
to switch.

### The gateway's HTTP API stays on the Pi

The `gateway` role's API binds to `127.0.0.1:8001`. That API can create
connections and write points, and Baler doesn't need it: Baler reaches a
gateway over the MQTT command channel (`cmd/<site-id>/<instance-id>/#`, see
[command-channel.md](command-channel.md)), the same outbound connection it
publishes on. To use the API from the Pi itself: `curl localhost:8001/devices`.

To expose it on the LAN anyway, set `WINDROW_GATEWAY_SECRET` in
`/etc/windrow/edge.env` (requests then need it in `X-Gateway-Secret`) and
reinstall with:

```bash
sudo WINDROW_API_HOST=0.0.0.0 bash install-edge.sh gateway
```

Installs from before this change bound the API to `0.0.0.0`; re-running the
script moves it to loopback.

## Broker account (weather, gateway)

These two publish to Baler, so they need an edge-node account. In Baler,
**Ops → Edge nodes → Add edge node** (admin), for a site with a topic
prefix. Use the site id as the username for a `gateway`, since its command
channel is `cmd/<username>/#`. Then put it in `/etc/windrow/edge.env` (mode
640) and `sudo systemctl restart windrow-<role>`:

```
MQTT_HOST=timberdoodle.nash.engineering
MQTT_PORT=8883            # TLS, verified against the system CA store
MQTT_USERNAME=...
MQTT_PASSWORD=...
WEATHER_TOPIC_PREFIX=fbf/home/weather   # must sit under the site's prefix
WINDROW_SITE_ID=...                     # gateway: the broker username
```

The broker drops anything published outside the account's site prefixes, so
a wrong `WEATHER_TOPIC_PREFIX` or a gateway connection's `topic_prefix`
outside the site looks like "connected but no data". Check the prefix first.

## When the uplink is down

Readings taken while the broker is unreachable are spooled to disk
(`WINDROW_SPOOL_FILE`, default `windrow-spool.jsonl` in the working
directory) and replayed, with their original timestamps, on reconnect. The
spool holds `WINDROW_SPOOL_OUTAGE_HOURS` (default 24) of readings at the
rate this node polls - every point's poll rate, BACnet and Modbus - plus
10%, between 10,000 rows and `WINDROW_SPOOL_MAX_ROWS` (default 5,000,000,
about 500 MB). It is resized as connections change. In an outage longer
than that, the oldest readings go first, and each trim is logged with how
many readings of how many points were dropped and the time span lost. The
status heartbeat's `spool` shows `rows`, `capacity` and `dropped`.

## Checking

```bash
systemctl status windrow-<role>
journalctl -u windrow-<role> -f
```

On a `gateway`, `curl -s localhost:8001/devices` lists what it has discovered.

From another machine, check the simulators are reachable:
- `bacnet`: a Who-Is from any BACnet tool, or add it as a connection in Baler's Devices page by IP.
- `modbus`: `nc -z <pi> 502`.

Re-running the script upgrades Windrow to the current `main`, or to
`WINDROW_REF=<branch, tag or full sha>`. It compares commits, not version
strings, so it upgrades even though the version stays `0.1.0`. It reinstalls
Windrow only when the commit differs, and installs only the dependencies that
are missing or need a newer version. It rewrites the unit, restarts the service
only if Windrow or the unit changed, and prints the installed commit.
`/etc/windrow/edge.env` is kept.
