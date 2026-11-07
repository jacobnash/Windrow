"""
Simulated BACnet router - and optionally BBMD - with mock devices behind it
on a separate, non-IP network (Baler#126). The test bed for discovering and
polling a device Windrow can only reach through a router, the way an MS/TP
trunk hangs off a BACnet/IP router in a real building.

One process:
- the router's BACnet/IP port, network --network (default 1), at --address;
  with --bbmd it is also a BBMD that accepts foreign-device registrations
  (bacpypes3's own BBMD), so a Windrow on another subnet can register with
  it and have its broadcasts distributed;
- a virtual network, --trunk-network (default 2), standing in for the MS/TP
  trunk: bacpypes3's in-process VLAN, one-byte MAC addresses like MS/TP;
- one windrow.mock_device per --device MAC:INSTANCE on that trunk (the
  default two points, simulated the same way). From Windrow's side each is
  `<trunk-network>:<mac>`, e.g. 2:5.

  python -m windrow.mock_router --address 192.168.1.40/24:47808 --bbmd --device 5:4242 --device 6:4243

What it doesn't simulate: MS/TP itself (token passing, timing, 480-byte
APDUs - the virtual network has none of that), or more than one router hop.
"""

import asyncio

from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.vlan import VirtualNetwork

from windrow.mock_device import MockDevice

TRUNK_NAME = "windrow-mock-trunk"


def _trunk_port(mac: int, network: int, instance: int = 1) -> NetworkPortObject:
    return NetworkPortObject(
        objectIdentifier=("network-port", instance),
        objectName=f"NetworkPort-{instance}",
        networkType="virtual",
        protocolLevel="bacnet-application",
        macAddress=bytes([mac]),
        networkNumber=network,
        networkNumberQuality="configured" if network else "unknown",
        networkInterfaceName=TRUNK_NAME,
        statusFlags=[0, 0, 0, 0],
        reliability="no-fault-detected",
        outOfService=False,
        changesPending=False,
        linkSpeed=0.0,
    )


def build(args) -> tuple[Application, list[MockDevice]]:
    """The router (from the usual bacpypes3 --address/--network/--bbmd
    args, plus a port on the trunk) and the devices behind it."""
    if not args.network:
        raise SystemExit("a router needs --network (its BACnet/IP network number)")
    VirtualNetwork(TRUNK_NAME)
    router = Application.from_args(args)
    router.add_object(_trunk_port(mac=1, network=args.trunk_network, instance=2))
    devices = []
    for spec in args.device or ["5:4242"]:
        mac, _, instance = spec.partition(":")
        app = Application.from_object_list([
            DeviceObject(objectIdentifier=("device", int(instance)), objectName=f"trunk-device-{mac}",
                         vendorIdentifier=args.vendoridentifier),
            _trunk_port(mac=int(mac, 0), network=0),
        ])
        devices.append(MockDevice(app=app))
    return router, devices


async def main() -> None:
    parser = SimpleArgumentParser()
    parser.add_argument("--trunk-network", type=int, default=2, help="network number of the simulated MS/TP trunk")
    parser.add_argument("--device", action="append", help="MAC:INSTANCE of a mock device on the trunk (repeatable; default 5:4242)")
    parser.set_defaults(network=1, instance=100, name="mock-router")
    args = parser.parse_args()
    running = build(args)  # router and devices: held so they aren't collected
    print(f"router on network {args.network} at {args.address}"
          f"{' (BBMD)' if args.bbmd is not None else ''}; trunk network {args.trunk_network}: "
          + ", ".join(f"{args.trunk_network}:{int(d.split(':')[0], 0)} = device {d.split(':')[1]}" for d in (args.device or ["5:4242"])),
          flush=True)
    await asyncio.Future()  # run forever
    del running


if __name__ == "__main__":
    asyncio.run(main())
