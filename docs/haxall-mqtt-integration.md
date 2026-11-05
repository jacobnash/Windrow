# Getting Windrow's MQTT data into Haxall: a runbook

Wiring `windrow.bridge`'s MQTT output into a real Haxall point's `curVal` looks
like it should be a two-line config. It isn't, because the connector,
observation, and task subsystems are each documented as if the other two
don't exist. Nothing here is obvious from Haxall's own docs — most of it
came from reading `.fan` source directly. This is the runbook so nobody has
to do that again.

## Prerequisites

- Windrow running (`windrow.mock_device` + `windrow.bridge`), publishing to an MQTT
  broker reachable from Haxall's container.
- A Haxall distribution (`haxall.io/download`), run in Docker with a JRE
  base image (`eclipse-temurin:21-jre`) — Haxall needs a JRE, not Fantom
  itself; you never write Fantom to do any of this.
- Haxall and the MQTT broker on the same Docker network, so Haxall can
  resolve the broker by container name.

## 1. Boot Haxall

```
hx init -headless -suUser su -suPass <pass> /haxall/var/demo
hx run /haxall/var/demo
```

Use `-noAuth` on `hx run` for faster local iteration — skips the login page
entirely for loopback connections.

**Gotcha**: `bin/hx run --debug <logger>` does not exist. That flag is from
a different tool's CLI (bacpypes3's argparse, if you've been looking at
both). Haxall's logging is controlled by `etc/sys/log.props`
(`loggerName=level`, one per line), and even then it needs the exact
internal logger name — not obviously discoverable, and not load-bearing for
this integration. Don't chase it.

## 2. Enable the lib dependency chain

The MQTT connector's dependencies are not auto-resolved — add them in this
exact order, or each `libAdd` fails with `Cannot add, missing depends: X`:

```
libAdd("ph")
libAdd("hx.point")
libAdd("hx.conn")
libAdd("hx.mqtt")
```

`hx.task` (below) may already be pulled in transitively — if `libAdd("hx.task")`
says "Lib already enabled," that's success, not an error.

## 3. Create the MQTT connector

```
commit(diff(null, {mqttConn, conn, dis:"My Conn", uri: `mqtt://<broker-host>:1883`}, {add}))
```

**Gotcha**: the URI scheme must be `mqtt://`, `mqtts://`, `ws://`, or `wss://`.
`tcp://` — the obvious guess — throws `Unsupported URI scheme` deep inside
`mqtt::Transport`. Nothing in the connector record's own validation catches
this at commit time; it only surfaces once the connector tries to open.

**Gotcha**: a connector doesn't open automatically. Force it:

```
connPing(<connId>)
```

Check `connState`/`connStatus` via `read(mqttConn)` — `open`/`ok` means it's
genuinely connected to the broker, not just configured.

## 4. Create target points

```
commit(diff(null, {point, cur, dis:"My Point", kind:"Number"}, {add}))
```

**Gotcha**: do not include `curVal`/`curStatus` at creation. They're
transient-only tags — a persistent commit that sets them fails with
`Cannot set tag persistently: "curVal"`. Set them later via a commit with
the `{transient}` flag (step 5).

## 5. Create the ingestion task

MQTT has no per-point `curAddr`-style address the way Modbus does — there is
no config that makes a point "just" subscribe to a topic. Incoming messages
surface through the **observable/task subsystem**, not the connector-point
subsystem. This is the single biggest undocumented fact in the whole chain.

```
commit(diff(null, {
  task,
  dis: "My Ingest Task",
  obsMqtt,
  obsMqttConnRef: <connId>,
  obsMqttTopic: "your/topic/here",
  taskExpr: "(msg) => do
    json: ioReadJson(msg->payload);
    commit(diff(readById(<pointId>), {curVal: json->value, curStatus: \"ok\"}, {transient}))
  end"
}, {add}))
```

**Gotcha (the big one)**: `parseJson` does not exist as an axon function.
The real one is `ioReadJson(handle)` — and it takes the raw `Buf` payload
directly, no manual `.readAllStr()` needed. Calling a nonexistent function
does not fail loudly anywhere visible. The task silently increments its
internal error counter forever, with zero indication anything is wrong
unless you go looking with the right tool (next section).

**Gotcha**: `read(task and dis==...)` will never show a `taskStatus` column,
even when everything is working. `taskStatus` is not a Folio-persisted tag —
it's an in-memory-only field on the live `Task` object. Checking for it via
`read()` looks exactly like "the task was never registered," which is a
convincing but wrong diagnosis.

## 6. Actually check whether it's working

Use the dedicated axon function, not `read()`:

```
tasks()
```

This returns `taskStatus`, `subscription`, `evalNum`, `errNum`, `errLast`.
If `errNum == evalNum`, the task is running and receiving messages, but
every single evaluation is throwing — check `errLast` (or
`taskDebugDetails(<taskRef>)` for the full trace) for the actual axon error.
This is the tool that would have saved the most time.

## Why this took so long, briefly

The connector framework (`hxConn`), the observation framework (`obs`), and
the task engine (`hxTask`) are three separately-designed subsystems. Each
one's docs assume you already know the other two. MQTT's connector doesn't
implement the `curAddr`/`onSyncCur` pattern that Modbus and most other
connectors use, so the "normal" way of wiring a connector to a point
(the thing every doc example shows) doesn't apply here at all — you have to
already know to route through tasks/observations instead, and nothing
tells you that until you read `MqttDispatch.fan` directly.

## Related, not yet needed

- Haxall ships an **open-source Haystack connector** (`hxHaystack`,
  announced in the same v3.1.3 release as Modbus/MQTT/SQL/oBIX). This means
  Haxall can pull data directly from any other Haystack-speaking server —
  including a Niagara station running NHaystack — with no new engineering.
  Not used in Windrow yet, worth knowing it's there.

## Known risk for a future Haystack HTTP API reader (not built yet)

Both Haxall and SkySpark support Haystack's SCRAM auth for their HTTP API —
but **it is not standard SASL SCRAM (RFC 5802)**, despite the name. Per
[this writeup](https://web.archive.org/web/20241118225408/https://www.alienfactory.co.uk/articles/skyspark-scram-over-sasl),
it's a custom variant:

- The SCRAM exchange is wrapped inside HTTP headers/params, with a server-issued
  `handshakeToken` the client must echo back — a bespoke session-tracking
  scheme layered on top of SCRAM, not a real SASL session.
- It uses **unpadded Base64URL** encoding (no trailing `=`), not standard
  padded Base64 — silently breaks generic crypto libraries that assume the
  RFC-standard encoding.
- The exchange spans multiple HTTP GET requests, not a single SASL negotiation.

A generic, spec-compliant SCRAM library (the kind that works fine against
Postgres or Kafka) will not authenticate here. Don't attempt a from-scratch
implementation against this — use `pyhaystack` or another client already
built for this specific variant, and confirm it actually handles the
handshakeToken/unpadded-base64url quirks before trusting it against a real
SkySpark instance. Basic or Bearer auth (if the target server allows it)
sidesteps this entirely and should be tried first.
