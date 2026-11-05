# Windrow — Windrow

Bridges BACnet devices to MQTT. No Fantom, no license fee, no walled garden.

Haxall (SkyFoundry's open-source BIoT framework) ships open connectors for
Modbus, MQTT, SQL, oBIX, and more — but BACnet has never been one of them in
five years of releases. It's the one thing you have to buy SkySpark for.

Windrow fills that gap from outside: it speaks BACnet directly (via
[BACpypes3](https://github.com/JoelBender/BACpypes3), BSD-licensed) and
publishes readings to MQTT, which Haxall's existing open `hxMqtt` connector
already knows how to ingest. No native Haxall connector, no Fantom code.

## Status

Early scaffold. See `plan.md` for the phased build plan.

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
`docs/bacnet-discovery-api.md`.) From there, baler's
`mqtt_listener.py` picks the readings/tags up unchanged.

`windrow.api` also runs periodic BACnet/Modbus discovery on its own and tracks
every device it's seen, provisioned or not, with optional per-device
credentials - see `docs/periodic-discovery-and-credentials.md`.

A central Baler deployment can also reach back into a site to
trigger a scan remotely, over the same outbound MQTT connection - see
`docs/command-channel.md`.

## License

AGPLv3 — see [LICENSE](LICENSE). Same terms as
[Baler](https://github.com/jacobnash/baler): self-host
freely, but a modified version run as a network service for others must
make its source available to them too.
