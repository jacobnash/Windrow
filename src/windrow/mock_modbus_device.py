"""
Simulated Modbus/TCP device for testing Windrow's Modbus bridge without real
hardware. Two changing holding registers, same spirit as mock_device.py's
BACnet analog/binary pair. Adapted from pymodbus's own
examples/server_updating.py.
"""

import argparse
import asyncio

from pymodbus.pdu.device import ModbusDeviceIdentification
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

ZONE_TEMP_ADDR = 0
FAN_STATUS_ADDR = 1
INTERVAL = 5.0


async def updating_task(server) -> None:
    func_code = 3  # holding registers
    readings = [(70, 1), (69, 0), (71, 1)]
    while True:
        await asyncio.sleep(INTERVAL)
        temp, fan = readings.pop(0)
        readings.append((temp, fan))
        await server.async_setValues(1, func_code, ZONE_TEMP_ADDR, [temp])
        await server.async_setValues(1, func_code, FAN_STATUS_ADDR, [fan])


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5020)
    args = parser.parse_args()

    # Gives modbus_discovery.sweep()'s read_device_information() something
    # real to find - a device that doesn't set this (most cheap real ones
    # don't) is still a valid discovery result, just with vendor/product/
    # revision all None.
    identity = ModbusDeviceIdentification(
        info_name={
            "VendorName": "Windrow Mocks",
            "ProductCode": "MOCK-MB-1",
            "MajorMinorRevision": "1.0",
        }
    )

    server = ModbusTcpServer(
        SimDevice(1, [
            SimData(ZONE_TEMP_ADDR, count=1, datatype=DataType.UINT16, values=70),
            SimData(FAN_STATUS_ADDR, count=1, datatype=DataType.UINT16, values=0),
        ]),
        address=(args.host, args.port),
        identity=identity,
    )

    task = asyncio.create_task(updating_task(server))
    try:
        await server.serve_forever()
    finally:
        task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
