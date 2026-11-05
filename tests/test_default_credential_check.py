"""
Uses a real stdlib http.server on a loopback port serving Basic Auth
200-for-admin/admin, 401-otherwise - a handful of lines, no new fixture
framework, same "live against something real, not mocked" spirit as this
project's other tests.
"""

import base64
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from windrow.default_credential_check import check


def _make_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            expected = "Basic " + base64.b64encode(b"admin:admin").decode()
            if self.headers.get("Authorization") == expected:
                self.send_response(200)
                self.end_headers()
            else:
                self.send_response(401)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_finds_the_matching_default_credential():
    server = _make_server()
    try:
        port = server.server_address[1]
        result = check("127.0.0.1", endpoints=[("http", port)])
        assert result is not None
        assert result["username"] == "admin"
        assert result["password"] == "admin"
        assert result["scheme"] == "http"
    finally:
        server.shutdown()


def test_returns_none_against_a_closed_port():
    assert check("127.0.0.1", endpoints=[("http", 1)]) is None
