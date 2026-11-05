# Comparison: the Haxall detour vs. Baler's direct path

Both paths now exist, live, for the same physical points (one BACnet AHU,
one Modbus AHU). This is what it actually cost to build each.

## Direct path (Phase 1, unmodified, still green)

```
device → protocol bridge → MQTT (fbf/{topic}) → Baler mqtt_listener → graph + timeseries
```

2 hops after the bridge. No intermediate system to provision, tag, or keep
alive. Proof: `baler/tests/test_ingest.py::test_full_round_trip_over_real_broker`
— publishes the real envelope over the real broker, asserts the value is
queryable in Postgres. No Haxall container involved anywhere in this test.

## Haxall path (Phases 1–3 of this plan)

```
device → protocol bridge → MQTT → Haxall lib chain (ph, hx.point, hx.conn,
hx.mqtt, hx.task) → mqttConn → obsMqtt task → axon taskExpr → Folio commit
→ Haystack tagging (site/equip/point model) → haystack_bridge.py (SCRAM
auth, axon eval/pointWrite) → MQTT (fbf/haxall-readback/{point}[/tags]) →
Baler mqtt_listener (+ /tags routing) → graph + timeseries
```

~8 layers, none of them optional:

1. **Lib chain** — 5 Haxall libraries must be loaded in the right order
   before anything else works (`provision_haxall.ensure_libs`).
2. **Connector + task engine** — no per-point `curAddr` for MQTT; every
   point needs a dedicated `task` record with `obsMqtt`/`obsMqttConnRef`/
   `obsMqttTopic` tags and hand-written axon (`ioReadJson()`, not the
   `parseJson()` a first guess would reach for — it doesn't exist).
3. **Nested-quoting in generated axon** — a `taskExpr` embedding a
   `value_expr` like `json->value == "active"` needs its quotes escaped
   before insertion into the already-double-quoted axon string. A real bug,
   not a hypothetical one — it shipped once and had to be fixed.
4. **Non-standard SCRAM auth** — Haxall's variant isn't RFC 5802 SASL
   SCRAM; a generic client won't authenticate. Full writeup:
   `docs/haxall-mqtt-integration.md`.
5. **Two separate GET-vs-POST landmines** — Haxall rejects GET for
   non-idempotent ops. Both `pyhaystack.get_eval()` and
   `pyhaystack.point_write()` issue GET and both 405 against this version;
   both had to be bypassed with a hand-built `_post_grid()` call.
6. **A wrong-by-default function signature** — axon's real `pointWrite()`
   is `(point, val, level, who, opts)`, not `(point, level, val)` like
   pyhaystack's own wrapper assumes. Discovered from the live error message
   after the first (wrong) call failed with `sys::Err: Invalid level: 42`.
7. **Haystack tagging model** — a site/equip/point structure has to exist
   and be tagged before there's anything meaningful to bridge back out.
8. **The bridge itself** — a second auth+eval client, a poll loop, a
   metadata side-channel (the `/tags` convention) invented because the
   frozen value envelope has no room for markers/refs.

Proof this path works end-to-end: `test_haxall_provision.py` (idempotent
provisioning + tagging, 3 tests) and `test_haystack_bridge.py` (read mode
lands both value and tag messages on the real broker; write-then-readback
round-trips a real value through Haxall's Folio) — all 8 integration tests
pass live, no mocks, against the actual `windrow-haxall` container.

## The point

The direct path was finished in Phase 1 with zero knowledge of any of the
above. Everything in items 1–8 is real cost that exists *only* because
Haxall's Folio + task engine + Haystack tagging sits in the middle — none
of it makes the data more correct, more current, or more available than
the direct path already provides. It exists to prove interoperability with
installs that already run Haxall/SkySpark, not because it's a better way
to get new data flowing.
