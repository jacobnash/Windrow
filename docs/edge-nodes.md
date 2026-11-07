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
| `gateway` | `windrow.api`, Windrow itself | discovers and polls BACnet/Modbus devices on this LAN, publishes to Baler |

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

## Checking

```bash
systemctl status windrow-<role>
journalctl -u windrow-<role> -f
```

From another machine, check the simulators are reachable:
- `bacnet`: a Who-Is from any BACnet tool, or add it as a connection in Baler's Devices page by IP.
- `modbus`: `nc -z <pi> 502`.

Re-running the script upgrades Windrow (`WINDROW_REF=<tag or sha>` pins it)
and rewrites the unit; `/etc/windrow/edge.env` is kept.
