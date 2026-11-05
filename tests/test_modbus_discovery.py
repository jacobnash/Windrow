"""
Live against a real windrow.mock_modbus_device subprocess on loopback (spawned
by the mock_modbus_device fixture in conftest.py).
"""

import asyncio
import socket

import pytest

from windrow.modbus_discovery import sweep


@pytest.mark.integration
def test_sweep_finds_device_and_reads_identity(mock_modbus_device):
    devices = asyncio.run(sweep("127.0.0.1/32", port=mock_modbus_device, timeout=2.0))

    assert len(devices) == 1
    assert devices[0]["host"] == "127.0.0.1"
    assert devices[0]["port"] == mock_modbus_device
    assert devices[0]["vendor"] == "Windrow Mocks"
    assert devices[0]["product_code"] == "MOCK-MB-1"


@pytest.mark.integration
def test_sweep_with_nothing_listening_returns_empty():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        closed_port = s.getsockname()[1]  # bound-and-released, nothing listening

    devices = asyncio.run(sweep("127.0.0.1/32", port=closed_port, timeout=1.0))

    assert devices == []
