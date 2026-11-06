"""
Throwaway manual check: discover the mock device over real BACnet/IP and
read its present-value points. Run the mock device first, then this.
"""

import asyncio

from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser


async def main() -> None:
    args = SimpleArgumentParser().parse_args()
    app = Application.from_args(args)
    try:
        i_ams = await app.who_is()
        print(f"discovered {len(i_ams)} device(s)")
        for i_am in i_ams:
            addr = i_am.pduSource
            dev_id = i_am.iAmDeviceIdentifier
            print(f"  device {dev_id} at {addr}")

            for obj_id, prop in [
                (("analogValue", 1), "present-value"),
                (("binaryValue", 1), "present-value"),
            ]:
                value = await app.read_property(addr, obj_id, prop)
                print(f"    {obj_id} {prop} = {value}")
    finally:
        app.close()


if __name__ == "__main__":
    asyncio.run(main())
