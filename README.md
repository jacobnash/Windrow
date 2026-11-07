# Windrow

Bridges BACnet devices to MQTT. No Fantom, no license fee, no walled garden.

Haxall (SkyFoundry's open-source BIoT framework) ships open connectors for
Modbus, MQTT, SQL, oBIX, and more — but BACnet has never been one of them in
five years of releases. It's the one thing you have to buy SkySpark for.

Windrow fills that gap from outside: it speaks BACnet directly (via
[BACpypes3](https://github.com/JoelBender/BACpypes3), BSD-licensed) and
publishes readings to MQTT, which Haxall's existing open `hxMqtt` connector
already knows how to ingest. No native Haxall connector, no Fantom code.

## Status

Windrow runs in production behind [Baler](https://github.com/jacobnash/Baler),
which pins it as a git submodule.

**Works today:**

- **BACnet/IP reads**: polls `present-value` per point on a fixed interval
  (`windrow.connection_manager`) and publishes to MQTT.
- **BACnet writes**: `WriteProperty(present-value)` at priority 1-16, with
  relinquish and timed overrides, from the HTTP API or Baler's command
  channel.
- **Modbus TCP reads**: holding registers, one 16-bit register per point
  (`windrow.modbus_connection_manager`).
- **Discovery**: periodic BACnet Who-Is and Modbus subnet scans, a registry
  of every device seen, and optional per-device credentials
  ([docs/periodic-discovery-and-credentials.md](docs/periodic-discovery-and-credentials.md)).
- **Command channel**: Baler can trigger scans, learn device point lists and
  write points on a site over the same outbound MQTT connection
  ([docs/command-channel.md](docs/command-channel.md)).
- **Raspberry Pi edge nodes**: `deploy/pi/install-edge.sh` sets up a Pi as a
  gateway (or as a mock BACnet/Modbus/weather device). Readings taken while
  the broker is unreachable are spooled to disk and replayed on reconnect ([docs/edge-nodes.md](docs/edge-nodes.md)).
- **Haystack servers as a source** (nhaystack, SkySpark, Haxall)
  ([docs/haystack-source.md](docs/haystack-source.md)).

**Not yet** (tracked in Baler's issue tracker):

- BACnet across subnets and MS/TP: no BBMD, foreign-device registration or
  router support, so Windrow only sees devices on its own broadcast domain
  (jacobnash/Baler#126).
- ReadPropertyMultiple and COV subscriptions: one ReadProperty per point,
  poll only (jacobnash/Baler#127).
- Modbus beyond holding registers: no input registers, coils, 32-bit/float
  data types, scale/offset, word order, or writes (jacobnash/Baler#128).
- BACnet trend logs, alarms (intrinsic reporting) and schedules
  (jacobnash/Baler#129).

Adding a new connector kind: [docs/adding-a-new-source.md](docs/adding-a-new-source.md).

## Setup

```
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Mock hospital site

`windrow.mock_hospital` generates a hospital-wing-sized mock BACnet site (~500
points across ~90 pieces of equipment - AHUs, VAVs, chillers, boilers,
pumps, meters, exhaust fans - modeled on Project Haystack's public "charlie"
example, with deliberately inconsistent point naming). Three processes:

```
# 1. the simulated BACnet device, loaded with the generated object list
python -m windrow.mock_hospital generate --out hospital_site_spec.json
python -m windrow.mock_device --address 192.168.128.63/24:47808 --instance 3456 --site-spec hospital_site_spec.json

# 2. the discovery/connections API (owns the BACnet client + MQTT publish)
python -m windrow.api --address 192.168.128.63/24:47820 --mqtt-host localhost

# 3. provision one connection per equipment against it
python -m windrow.mock_hospital provision --device-address 192.168.128.63:47808 --device-instance 3456
```

(Adjust `--address`/`--device-address` to your own host's interface -
they're bacpypes3's own local-BACnet-address arg, same as
`docs/bacnet-discovery-api.md`.) From there, Baler's
`mqtt_listener.py` picks the readings/tags up unchanged.

`windrow.api` also runs periodic BACnet/Modbus discovery on its own and tracks
every device it's seen, provisioned or not, with optional per-device
credentials - see `docs/periodic-discovery-and-credentials.md`.

A central Baler deployment can also reach back into a site to
trigger a scan remotely, over the same outbound MQTT connection - see
`docs/command-channel.md`.

## License

AGPLv3 — see [LICENSE](LICENSE). Same terms as
[Baler](https://github.com/jacobnash/Baler): self-host
freely, but a modified version run as a network service for others must
make its source available to them too.
