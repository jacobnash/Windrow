# Haystack servers as a source (nhaystack, SkySpark, Haxall)

`windrow.haystack_source` reads any Project Haystack HTTP server and
publishes its points into the same MQTT feed as BACnet and Modbus, so
Baler ingests them unchanged. The main target is a **Niagara station
running nhaystack**. That gets Niagara data into Baler with no Niagara
code, no Fox protocol, and no SkySpark.

```bash
pip install -e ".[haystack]"      # openhay (needs cargo to build)
```

```bash
python -m windrow.haystack_source \
  --url https://station.example/haystack --user admin --password '...' \
  --source-id hospital-station \
  --mqtt-host broker --mqtt-username hq --mqtt-password '...' \
  --site-id hq --instance-id hospital-station \
  --backfill yesterday
```

- `--url`: the Haystack API root. For nhaystack that's the station's
  Haystack servlet; for SkySpark it's `.../api/<project>`; for Haxall it's
  `.../api`.
- **Auth:** SCRAM with `--user`/`--password`, or `--bearer <token>` for
  servers that issue OAuth/API keys.
- `--filter`: which records to bridge (default `point`).
- `--backfill`: a `hisRead` range (`yesterday`, `2026-11-01,2026-11-05`, …)
  replayed once on start, with original timestamps.
- `--site-id`: turns on the command channel, so Baler can write points
  (see below).

## What it publishes

- **Equipment:** each piece of equipment becomes a topic prefix
  `fbf/<source-id>/<equip>`, where the equip name is its `navName` or
  `dis`, slugged. Points with no `equipRef` go under
  `fbf/<source-id>/misc`.
- **Points:** each point is `{prefix}/{label}`, where the label is the
  point's `navName` or `dis`, slugged and made unique within its
  equipment.
- **Tags:** every tag except live and bookkeeping fields (`id`, `mod`,
  `curVal`, refs, ...) goes to `{prefix}/{label}/tags`, along with
  `sourceId`, the original Haystack id. Niagara-only tags such as `axType`,
  `axSlotPath` and `axHistoryId` pass through as ordinary string tags.
- **Values:** live values come from `watchSub`/`watchPoll`; servers without
  watches fall back to polling `read`. Only changes are published.

## Writing back (Baler pointWrite to a Niagara point)

With `--site-id`, the source listens on `cmd/<site>/<instance>/#` and
handles the same `point_write` action BACnet connections do (see
[command-channel.md](command-channel.md)). It performs a Haystack
`pointWrite` on the server at the same level, with `null` relinquishing
it. Its equipment tags advertise `windrowSite`/`windrowInstance`, so a
Baler `pointWrite` on one of these points travels Baler → broker →
Windrow → nhaystack → the Niagara point.

Only points the server tags `writable` are accepted (nhaystack tags
writable Niagara points that way). Expired commands are refused, as for
BACnet.

## Testing without a station

`tests/fixtures/nhaystack_station.trio` is a small station shaped like
nhaystack's output. That covers `S.`-prefixed slot-path ids, history ids,
Niagara metadata tags, and writable Numeric/Boolean points. It's built
from nhaystack's public (AFL-3.0) source and docs, not recorded from a
real station. The tests serve it with openhay's `hay serve` and run
discovery, watches, backfill and write-back over real HTTP and SCRAM.
Once a Niagara station (for example Tridium's containerized Niagara) is
available, record real responses and replace the fixture.
