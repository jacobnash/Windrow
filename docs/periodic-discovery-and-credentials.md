# Periodic discovery, the device registry, and credentials

Built for new-construction commissioning: equipment shows up on the network
over time, and a human shouldn't have to remember to click "scan." `windrow.api`
now runs discovery on its own on a timer, tracks every device either scan
has ever seen — separate from provisioned connections — and gives you
somewhere to record a login if a device needs one.

```
python -m windrow.api --address 192.168.128.63/24:47820 --mqtt-host localhost \
  --bacnet-discovery-interval 300 \
  --modbus-scan-cidr 192.168.1.0/24 --modbus-scan-interval 600
```

- BACnet Who-Is scanning always runs (`--bacnet-discovery-interval`,
  default 300s) — no subnet argument needed, it broadcasts on whatever
  interface `--address` already bound.
- Modbus scanning is **off by default**. Modbus has no discovery protocol
  of its own — "scan" means TCP-connect-sweeping a subnet on port 502 —
  so there's no safe range to guess. Pass `--modbus-scan-cidr` (repeatable)
  to turn it on for one or more subnets.

## 1. See what's been found

```
curl localhost:8001/devices
```

```json
[
  {
    "id": "7a1ac077179748588d981ef714161bda",
    "protocol": "bacnet",
    "device_instance": 3456,
    "address": "192.168.128.167",
    "vendor_id": 999,
    "status": "pending",
    "connection_id": null,
    "credential": null,
    "default_credential_flag": null,
    "default_credential_checked_at": null,
    "first_seen_at": 1786888550.46,
    "last_seen_at": 1786888550.46
  }
]
```

`status` is `pending` (seen, not provisioned), `provisioned` (a matching
connection exists — flips automatically, no extra call needed once you
`POST /connections` or `POST /modbus/connections` for it), or `ignored` (a
human dismissed it via `PATCH /devices/{id}`, survives future rescans).

## 2. Set a credential, if the device needs one

Neither BACnet nor Modbus has protocol-level auth — this is pure
bookkeeping for whatever manual login the device's own surface needs (a
gateway's local web UI, most commonly), recordable before any connection
exists for it:

```
curl -X PATCH localhost:8001/devices/<id>/credential -d '{"username": "admin", "password": "sup3r-secret"}'
```

```json
{"username": "admin", "password": "sup3r-secret"}
```

The plaintext password is shown **once**, in this response only. Every
later `GET /devices` (or `GET /connections` / `GET /modbus/connections`,
which support the same `PATCH .../credential`) redacts it:

```json
{"username": "admin", "password": null}
```

Stored encrypted at rest (Fernet, via the `cryptography` package), keyed by
the `WINDROW_CREDENTIAL_KEY` env var — generate one with:

```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Setting a credential is bookkeeping only — nothing here logs into the
device with it. That's deliberately out of scope; there's no protocol
handshake to wire it into yet.

## 3. Default-credential check

The first time a **Modbus** device is seen as `pending`, `windrow.api`
automatically tries a short list of common defaults (`admin/admin`,
`admin/password`, `root/root`, …) as HTTP Basic Auth against the device's
own address, on ports 80 and 443 — GET only, non-destructive, meant to
catch equipment still on factory defaults during commissioning:

```json
"default_credential_flag": {"username": "admin", "password": "admin", "scheme": "http", "checked_at": 1786888550.46}
```

**BACnet devices always show `default_credential_flag: null`.** BACnet has
no HTTP-login concept of its own, so there's nothing to check — this is
explicit, not a silently-skipped feature. Re-run the check later (e.g.
after changing the device's password) with:

```
curl -X POST localhost:8001/devices/<id>/recheck-credentials
```

Known gap: HTTP Basic Auth only. Real gateway firmware more often uses a
form-based admin login, which this doesn't catch — would need per-vendor
knowledge, out of scope for a *basic* check.

## 4. Modbus: discover and provision

Modbus has no self-describing object list like BACnet's — register
addresses are opaque and vendor-documented, so you supply `{label:
address}` directly instead of a `/learn` step:

```
curl -X POST localhost:8001/modbus/discover -d '{"cidr": "192.168.1.0/24"}'
```

```json
[{"host": "192.168.1.50", "port": 502, "vendor": "Acme Controls", "product_code": "X1-2000", "revision": "2.0"}]
```

`vendor`/`product_code`/`revision` come from Modbus function 0x2B/0x0E
(Read Device Identification) where the device implements it — `null` on a
device that doesn't is a normal result, not an error.

```
curl -X POST localhost:8001/modbus/connections -d '{
  "host": "192.168.1.50",
  "topic_prefix": "fbf/modbus-ahu-1",
  "poll_interval": 5.0,
  "points": {"zone-temp": 0, "fan-status": 1}
}'
```

Same `GET`/`DELETE .../{id}` and `PATCH .../{id}/credential` shape as
BACnet connections.

## Known v1 limitations

- **Same-host broadcast quirk as `/discover`.** A locally-originated
  BACnet Who-Is broadcast doesn't reliably loop back to another process on
  the *same* machine (macOS/BSD networking behavior) — the periodic BACnet
  scan can look like it's finding nothing if you're testing against a mock
  device on the same host. Works normally against real devices on a real
  network. Verified live: this is genuinely non-deterministic on a single
  host, not a bug in the scan loop itself (confirmed by driving
  `device_registry.reconcile_bacnet()` directly against a real, reliable
  *directed* discovery call — the reconciliation logic behaves correctly
  every time; only the same-host broadcast delivery is flaky).
- **One shared poll interval per Modbus connection**, not per-point like
  BACnet connections support (`his_collect_cov` etc.) — nothing asked for
  that yet, and Modbus's opaque register model doesn't get much from it.
- **No default-credential check for BACnet** — see above, not a gap so
  much as "the check has nowhere to point itself."
- **No file locking** on `windrow-devices.json` / `windrow-modbus-connections.json`
  — same accepted single-writer assumption as `connections.json`.

## Real bugs this was verified against, not just assumed to work

- **Device-registry reconciliation never matched a provisioned connection
  back to its discovered device.** `discover_devices()`'s own `address`
  field never carries a port ("no port — the standard BACnet/IP port 47808
  is implied"), but a connection's `device_address` field commonly does
  (`"192.168.128.63:47808"`, matching every worked example in this repo) —
  the two never compared equal. Fixed by normalizing on the host portion
  before matching. Found by actually provisioning a connection against a
  live mock device and watching `status` never leave `pending`, not by
  reading the code.
- **An unclosed `AsyncModbusTcpClient` stalls process shutdown
  indefinitely.** pymodbus's async client spawns its own internal
  "transport reconnect" background task; if nothing calls `client.close()`
  before the owning `asyncio.run()` call tears down, that task doesn't
  respond to cancellation in time and the whole shutdown hangs forever.
  Reproduced reliably with two sequential `asyncio.run()` calls in one
  process (exactly `windrow.api`'s create-a-connection-then-restart shape);
  fixed with an explicit `ModbusConnectionManager.close()` called from
  `api.py`'s shutdown path (and from every test's teardown).
- pymodbus 3.14's real method is `read_device_information(device_id=...)`,
  not `read_device_identification` — confirmed via `inspect` against the
  installed package rather than guessed from older docs/examples.
