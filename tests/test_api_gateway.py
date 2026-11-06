"""The API behind Baler's gateway (Baler#14): with WINDROW_GATEWAY_SECRET
set, only requests carrying the gateway's X-Gateway-Secret get through,
and there's no wildcard CORS. Rejected requests never reach the managers,
so none are needed here."""

import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from windrow.api import make_handler


@pytest.fixture
def serve(monkeypatch):
    servers = []

    def start(**env):
        for k in ("WINDROW_GATEWAY_SECRET", "WINDROW_CORS_ORIGIN"):
            monkeypatch.delenv(k, raising=False)
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(None, None, None, None))
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start
    for s in servers:
        s.shutdown()


def _get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers)


@pytest.mark.parametrize("method", ["GET", "POST", "PATCH", "DELETE"])
def test_direct_requests_refused_when_behind_gateway(serve, method):
    base = serve(WINDROW_GATEWAY_SECRET="s3cret")
    req = urllib.request.Request(f"{base}/devices", method=method, data=b"{}" if method != "GET" else None)
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=5)
    assert exc.value.code == 401


def test_wrong_secret_refused_spec_public(serve):
    base = serve(WINDROW_GATEWAY_SECRET="s3cret")
    assert _get(f"{base}/devices", {"X-Gateway-Secret": "nope"})[0] == 401
    assert _get(f"{base}/openapi.yaml")[0] == 200


def test_no_wildcard_cors_by_default(serve):
    base = serve(WINDROW_GATEWAY_SECRET="s3cret")
    _status, headers = _get(f"{base}/openapi.yaml")
    assert "Access-Control-Allow-Origin" not in headers


def test_cors_only_for_the_configured_origin(serve):
    base = serve(WINDROW_CORS_ORIGIN="http://localhost:8004")
    _status, headers = _get(f"{base}/openapi.yaml")
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:8004"
