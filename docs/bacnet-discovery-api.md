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
