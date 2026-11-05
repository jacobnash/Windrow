"""
Best-effort check for a discovered device still sitting on a factory-default
login - meant to catch equipment left unconfigured during commissioning, not
a general credential-cracking tool.

Neither BACnet nor Modbus has a login of its own (confirmed against both
protocol client libraries: Who-Is/I-Am and register reads are unauthenticated
by design) - so "default credentials" can only mean a discovered device's own
HTTP admin UI, the concrete case this feature was asked for (a gateway's
local web login). That's why this takes a bare host, not a protocol - it's
the same check regardless of whether the host came from a BACnet or a Modbus
discovery pass.

Deliberately narrow: HTTP Basic Auth only, GET only, a short hardcoded
default list, and it only ever runs against a host that already came out of
an authorized discovery scan - it has no other input, so it can't reach
anything the operator didn't already point discovery at. Known, accepted
gap: misses form-based admin logins, which are more common in real gateway
firmware than HTTP Basic Auth - catching those needs per-vendor login-form
knowledge, out of scope for this basic check.
"""

import time

import requests

from windrow import tracing

tracer = tracing.get_tracer(__name__)

DEFAULT_CREDENTIALS = [
    ("admin", "admin"),
    ("admin", "password"),
    ("admin", ""),
    ("root", "root"),
    ("user", "user"),
]
DEFAULT_ENDPOINTS = [("http", 80), ("https", 443)]
TIMEOUT_SECONDS = 3.0


def check(host: str, endpoints: list[tuple[str, int]] | None = None) -> dict | None:
    """Tries each default over HTTP then HTTPS, stopping at the first
    response that isn't a rejection (401/403) or a server error (5xx) - a
    200/301/302/etc on an auth'd GET is treated as "that credential got
    us in." Returns None if every attempt on every scheme is rejected or
    the host has nothing listening on 80/443 at all.

    endpoints overrides the (scheme, port) pairs tried - real callers never
    pass it (device_registry.py always uses the real DEFAULT_ENDPOINTS),
    it exists so tests can point this at a throwaway loopback port instead
    of needing root to bind 80/443."""
    endpoints = endpoints if endpoints is not None else DEFAULT_ENDPOINTS
    with tracer.start_as_current_span("default_credential_check.check") as span:
        span.set_attribute("host", host)
        for scheme, port in endpoints:
            for username, password in DEFAULT_CREDENTIALS:
                try:
                    resp = requests.get(
                        f"{scheme}://{host}:{port}/",
                        auth=(username, password),
                        timeout=TIMEOUT_SECONDS,
                        verify=False,
                    )
                except requests.RequestException:
                    continue
                if resp.status_code not in (401, 403) and resp.status_code < 500:
                    result = {"username": username, "password": password, "scheme": scheme, "checked_at": time.time()}
                    span.set_attribute("found", True)
                    return result
        span.set_attribute("found", False)
        return None
