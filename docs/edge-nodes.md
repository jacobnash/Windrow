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
#WINDROW_BACNET_FOREIGN_BBMD=10.1.0.1   # gateway: BACnet devices on other subnets, via this BBMD
#WINDROW_BACNET_FOREIGN_TTL=300
```

A gateway Pi only hears BACnet broadcasts on its own subnet. If the site's
controllers are on other subnets, point it at the site's BBMD with
`WINDROW_BACNET_FOREIGN_BBMD` (see bacnet-discovery-api.md, "Other subnets
and MS/TP"); MS/TP devices behind routers are found either way.

The broker drops anything published outside the account's site prefixes, so
a wrong `WEATHER_TOPIC_PREFIX` or a gateway connection's `topic_prefix`
outside the site looks like "connected but no data". Check the prefix first.

## Device announce: "I am here"

Field devices move: a DHCP lease changes, a controller is swapped, a
subnet is renumbered. A connection pinned to the old address then polls
nothing. Give the connection a **`device_name`**, and Windrow keeps it
pointed at wherever that device is now.

There are two sources for the address, and an announcement always wins:

1. **Announce.** A device that can run Python (a Pi next to the
   equipment, or the Pi *as* the equipment, as in these demos) runs
   `python -m windrow.announce`. It sends a small UDP datagram to its
   Windrow every 30 s, and at once when its own address changes:

   ```
   sudo WINDROW_ANNOUNCE_TO=192.168.128.230 bash install-edge.sh announce
   ```

   This announces the Pi's hostname, plus the bacnet/modbus roles
   installed on it (port, BACnet instance, Modbus unit). It needs no broker
   account.

2. **DNS.** For equipment that can't run anything, set `device_name` to its
   DNS name. Windrow re-resolves it every `--resolve-interval` seconds
   (default 300).

The receiving Windrow listens on `--announce-port` (the gateway role
uses 47900). It only accepts sources inside `--announce-allow` (the
gateway role uses its own LAN subnet), and it takes the device's address
from the datagram's **source IP**, never from the payload. Set the same
`WINDROW_ANNOUNCE_KEY` in `edge.env` on both ends to require
HMAC-signed announcements; signed ones older than 5 minutes are refused.

When the address behind a `device_name` changes, every BACnet and Modbus
connection with that name is re-pointed through the normal edit path,
which validates, persists and restarts its poll. Re-pointing keeps the
BACnet port and prefix length.

Two places show it:
- `GET /announced` lists every announced device: address, services,
  online (false after 3 missed intervals), last seen, and a history of
  address changes.
- The status heartbeat carries `announced`, `announced_online` and
  `announce_rejected`, so Baler's Connectors page can show them.

Set the name when you create the connection, or later:

```
PATCH /connections/{id}         {"device_name": "bacnet"}
PATCH /modbus/connections/{id}  {"device_name": "modbus"}
```

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
`WINDROW_REF=<branch, tag, or full or short sha>`, which it resolves to the
full commit and pins. It compares commits, not version strings, so it upgrades
even though the version stays `0.1.0`. It reinstalls Windrow only when the
commit differs, and installs only the dependencies that are missing or need a
newer version. It rewrites the unit and restarts that role if the unit changed.

All roles on a Pi share one install (`/opt/windrow`). So after any install,
the script restarts **every** running `windrow-*` service whose process started
before that install, whichever role you re-ran. Re-running for one role
upgrades them all. It ends by listing each service's state and start time.
`/etc/windrow/edge.env` is kept.
