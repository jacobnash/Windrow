"""
Shared test fixtures. windrow.mock_modbus_device is a plain TCP listener (unlike
the BACnet mock device, which needs a real bound network interface and is
expected to already be running per the README) - cheap enough to spawn per
test that needs one.
"""

import socket
import subprocess
import sys
import time

import pytest


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"mock modbus device never came up on port {port}")


@pytest.fixture
def mock_modbus_device():
    """Yields the port a fresh windrow.mock_modbus_device subprocess is
    listening on."""
    port = _free_port()
    proc = subprocess.Popen([sys.executable, "-m", "windrow.mock_modbus_device", "--port", str(port)])
    try:
        _wait_for_port(port)
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)
