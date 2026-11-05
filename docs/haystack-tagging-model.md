# Haystack tagging model

Scoping decision: this tags a small, honest building model (1 site, 2 AHUs,
the 4 real points already flowing through Phase 1's ingestion), not
project-haystack.org's full 184-equipment/1842-point reference dataset —
that dataset describes far more devices than we actually have behind it. A
larger multi-site "campus" test against the real reference datasets is
deferred future work (see the main plan), not built here.

## Model

```
Windrow Demo Site (site)
├── AHU-1 (equip, ahu)
│   ├── Zone Temp        (BACnet)  → zone, air, temp, sensor
│   └── Fan Status       (BACnet)  → fan, run, sensor
└── AHU-2 (equip, ahu)
    ├── Modbus Zone Temp (Modbus)  → zone, air, temp, sensor
    └── Modbus Fan Status(Modbus)  → fan, run, sensor

Windrow Write Demo (point, writable) — no equipRef, no siteRef
```

## Tag choices, against Haystack convention

- **`zone`, `air`, `temp`, `sensor`** — the standard Haystack marker-tag
  combination for a zone air temperature reading (matches
  `zoneAirTempSensor` conventions in the project-haystack.org docs).
- **`fan`, `run`, `sensor`** — standard combination for a fan run/status
  sensor point.
- **`equipRef`** on each of the 4 points, pointing at whichever AHU it
  belongs to — the standard way Haystack links a point to its equipment.
- **`ahu`** marker on the 2 equip recs — standard Haystack equipment marker
  for an air handling unit.
- **`Windrow Write Demo`** is deliberately outside the building model (no
  `equipRef`) and carries `writable` instead of `cur` — it exists solely so
  Phase 3's write-then-readback proof never races the live 5-second
  ingestion tasks running against the real 4 points.

## Idempotency

`provision_haxall.py`'s `ensure_site`/`ensure_equip`/`ensure_point_tags`
follow the same check-before-commit pattern as the rest of the script:
site/equip lookups by `dis`, point tagging by checking which markers/refs
are already present on the rec before committing only what's missing.
Verified by rerunning the whole script twice against a live Haxall instance
and by `test_tagging_model_is_idempotent_and_filterable` in
`tests/test_haxall_provision.py`.
