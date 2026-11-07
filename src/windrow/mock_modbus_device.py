"""
Simulated Modbus/TCP device for testing Windrow's Modbus bridge without real
hardware. Adapted from pymodbus's own examples/server_updating.py.

Register map (0-based protocol addresses; unit id 1). The first two are the
original pair - same spirit as mock_device.py's BACnet analog/binary - and
read the same as ever with the bare-address point form. The rest cover each
function code and data type Windrow reads (Baler#128), with the point
object that reads each one correctly in POINTS:

  holding 0     zone temp, uint16, whole °F (changes every INTERVAL)
  holding 1     fan status, uint16 0/1 (changes every INTERVAL)
  holding 2-3   supply air temp, float32 °C, big word order (changes)
  holding 4     zone temp setpoint, int16 tenths of °F (writable)
  holding 6-7   energy total, uint32 Wh, little word order (counts up)
  holding 10-11 spare, never changed by the mock (for write tests)
  input 0       outside air temp, int16 tenths of °C (negative)
  coil 0        fan command (writable)
  discrete 0    filter alarm
"""

import argparse
import asyncio
import struct

from pymodbus.pdu.device import ModbusDeviceIdentification
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

ZONE_TEMP_ADDR = 0
FAN_STATUS_ADDR = 1
SUPPLY_TEMP_ADDR = 2
ZONE_SP_ADDR = 4
ENERGY_ADDR = 6
SPARE_ADDR = 10
INTERVAL = 5.0

# Points that read this device's registers as what they are - what a
# human would type into a connection for it (see the module docstring).
POINTS = {
    "zone_temp": ZONE_TEMP_ADDR,
    "fan_status": FAN_STATUS_ADDR,
    # °C on the wire, published in °F: 12.8 °C reads as 55.04 °F.
    "discharge_air_temp": {"register": SUPPLY_TEMP_ADDR, "type": "float32", "scale": 1.8, "offset": 32, "unit": "°F"},
    "zone_temp_sp": {"register": ZONE_SP_ADDR, "type": "int16", "scale": 0.1, "unit": "°F", "writable": True},
    "energy": {"register": ENERGY_ADDR, "type": "uint32", "word_order": "little", "unit": "Wh"},
    "outside_air_temp": {"register": 0, "function": "input", "type": "int16", "scale": 0.1, "unit": "°C"},
    "fan_cmd": {"register": 0, "function": "coil", "writable": True},
    "filter_alarm": {"register": 0, "function": "discrete"},
}


def _uint32_little(value: int) -> list[int]:
    high, low = struct.unpack(">HH", struct.pack(">I", value))
    return [low, high]


async def updating_task(server) -> None:
    func_code = 3  # holding registers
    readings = [(70, 1, 12.8), (69, 0, 13.0), (71, 1, 12.5)]
    energy = 70000
    while True:
        await asyncio.sleep(INTERVAL)
        temp, fan, supply = readings.pop(0)
        readings.append((temp, fan, supply))
        energy += 25
        await server.async_setValues(1, func_code, ZONE_TEMP_ADDR, [temp])
        await server.async_setValues(1, func_code, FAN_STATUS_ADDR, [fan])
        await server.async_setValues(1, func_code, SUPPLY_TEMP_ADDR, list(struct.unpack(">HH", struct.pack(">f", supply))))
        await server.async_setValues(1, func_code, ENERGY_ADDR, _uint32_little(energy))


def make_device() -> SimDevice:
    """Separate coil / discrete-input / holding / input blocks, as most
    devices have (pymodbus's tuple order)."""
    return SimDevice(1, (
        [SimData(0, count=1, datatype=DataType.BITS, values=[1])],  # coil 0: fan command on
        [SimData(0, count=1, datatype=DataType.BITS, values=[0])],  # discrete 0: filter alarm clear
        [
            SimData(ZONE_TEMP_ADDR, count=1, datatype=DataType.UINT16, values=70),
            SimData(FAN_STATUS_ADDR, count=1, datatype=DataType.UINT16, values=0),
            SimData(SUPPLY_TEMP_ADDR, count=1, datatype=DataType.FLOAT32, values=12.8),
            SimData(ZONE_SP_ADDR, count=1, datatype=DataType.INT16, values=720),
            SimData(ENERGY_ADDR, count=2, datatype=DataType.UINT16, values=_uint32_little(70000)),
            SimData(SPARE_ADDR, count=2, datatype=DataType.UINT16, values=[0, 0]),
        ],
        [SimData(0, count=1, datatype=DataType.INT16, values=-45)],  # input 0: -4.5 °C
    ))


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
        make_device(),
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
