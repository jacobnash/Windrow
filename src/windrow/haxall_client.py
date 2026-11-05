"""
Shared Haxall HTTP API client: auth + axon eval. Used by provision_haxall.py
now and haystack_bridge.py later - one auth implementation, not two.

Verified live against a real Haxall instance:
- The project name in API paths is "sys", not whatever directory name was
  passed to `hx init`/`hx run` - Haxall's single-runtime API always exposes
  "sys", confirmed empirically (curl to /api/demo/... 404s, /api/sys/...
  doesn't).
- pyhaystack's `SkysparkScramHaystackSession` authenticates cleanly against
  Haxall (not just SkySpark) via the documented non-standard SCRAM variant.
- pyhaystack's own `get_eval()` is broken against this Haxall version: it
  issues a GET, and Haxall now rejects GET for the non-idempotent `eval` op
  (405). Call `_post_grid("eval", ...)` directly instead - the underlying
  primitive works fine, only the GET-based convenience wrapper doesn't.
- Same 405-on-GET problem hits pyhaystack's `point_write()` (also
  `_get_grid`-based) - confirmed live (`405 GET not allowed for op
  'pointWrite'`). Use axon's `pointWrite()` function via `eval_axon`
  instead. Its real signature, discovered from the live error message
  when guessing wrong, is `pointWrite(point, val, level, who, opts)` -
  **val before level** - not `(point, level, val)` like pyhaystack's own
  wrapper assumes.
"""

import hszinc
from pyhaystack.client.skyspark import SkysparkScramHaystackSession


def connect(uri: str, username: str, password: str, project: str = "sys") -> SkysparkScramHaystackSession:
    return SkysparkScramHaystackSession(uri=uri, username=username, password=password, project=project)


def eval_axon(session: SkysparkScramHaystackSession, expr: str) -> hszinc.Grid:
    """Raises pyhaystack.exception.HaystackError on axon-side failure -
    the exact class of error that went silent for hours in the earlier
    MQTT task integration; here it's a real Python exception, not a
    silently-incrementing counter."""
    grid = hszinc.Grid()
    grid.column["expr"] = {}
    grid.append({"expr": expr})
    op = session._post_grid("eval", grid, callback=lambda *a, **k: None)
    op.wait(timeout=15)
    return op.result


def _axon_literal(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f'"{value}"'


def point_write(session, point_ref: str, value, level: int = 8) -> None:
    """Writes via axon's pointWrite(), not pyhaystack's broken GET-based
    HTTP op - see the module docstring."""
    eval_axon(session, f"pointWrite(readById(@{point_ref}), {_axon_literal(value)}, {level})")
