"""
Modbus device discovery - the Modbus counterpart to discovery.py, but
necessarily shaped differently: Modbus has no Who-Is/I-Am broadcast, no
discovery protocol at all. "Find devices" here means TCP-connect-scanning a
user-supplied CIDR on port 502 and, where the device implements it,
best-effort reading its Basic Device Identification (function 0x2B/0x0E) for
vendor/product info. A device that doesn't implement 0x2B/0x0E is still a
real find - it just came back with vendor/product_code/revision all None.
"""

import asyncio
import ipaddress

import pymodbus.client as ModbusClient

from windrow import tracing

tracer = tracing.get_tracer(__name__)

DEFAULT_PORT = 502
DEFAULT_CONCURRENCY = 32
DEFAULT_TIMEOUT = 2.0


def _decode(info: dict, object_id: int) -> str | None:
    raw = info.get(object_id)
    if not raw:
        return None
    return raw.decode(errors="replace") or None


async def _probe_one(host: str, port: int, timeout: float) -> dict | None:
    client = ModbusClient.AsyncModbusTcpClient(host, port=port, timeout=timeout, retries=1)
    try:
        connected = await client.connect()
        if not connected:
            return None
        device = {"host": host, "port": port, "vendor": None, "product_code": None, "revision": None}
        try:
            info = await client.read_device_information(device_id=1)
            if not info.isError():
                device["vendor"] = _decode(info.information, 0)
                device["product_code"] = _decode(info.information, 1)
                device["revision"] = _decode(info.information, 2)
        except Exception:
            pass  # best-effort only - plenty of real devices don't implement 0x2B/0x0E
        return device
    except Exception:
        return None
    finally:
        client.close()


async def sweep(
    cidr: str,
    port: int = DEFAULT_PORT,
    concurrency: int = DEFAULT_CONCURRENCY,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict]:
    """TCP-connects every host in cidr on port, bounded to `concurrency`
    simultaneous attempts - an unbounded gather() over a /24 (254 hosts)
    opening sockets at once is a self-inflicted fd/ephemeral-port problem,
    the same reason connection_manager.py jitters its poll-task starts."""
    with tracer.start_as_current_span("modbus_discovery.sweep") as span:
        span.set_attribute("cidr", cidr)
        span.set_attribute("port", port)

        hosts = [str(ip) for ip in ipaddress.ip_network(cidr, strict=False).hosts()]
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded(host: str) -> dict | None:
            async with semaphore:
                return await _probe_one(host, port, timeout)

        results = await asyncio.gather(*(bounded(host) for host in hosts))
        devices = [d for d in results if d is not None]
        span.set_attribute("host_count", len(hosts))
        span.set_attribute("device_count", len(devices))
        return devices
