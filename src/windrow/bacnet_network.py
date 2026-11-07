"""
BACnet beyond the local subnet (Baler#126): BBMD / foreign-device
registration, routers, and routed (network-number) device addresses.

Most of the protocol is bacpypes3's - this module is the configuration,
visibility and addressing around it:

- Foreign-device registration and BBMD mode are bacpypes3's --foreign/--ttl
  and --bbmd options (BIPForeign registers, renews at the TTL and retries
  every 5 s until the BBMD acks). add_network_args gives them Windrow env
  vars, so an edge node's EnvironmentFile can set them, and network_status
  reports what the link layer is actually doing (registered or not), which
  bacpypes3 otherwise only logs at debug level.
- Discovery uses a global broadcast by default (discovery.py), which
  routers forward to every network behind them - MS/TP trunks included -
  and which a foreign device sends through its BBMD. discover_routers asks
  Who-Is-Router-To-Network for the network map.
- A device behind a router is addressed `net:mac` - `5:12` for MS/TP MAC 12
  on network 5, `5:10.0.1.20` for a BACnet/IP device on another IP network
  behind a router. bacpypes3 finds the router for `net` itself (a cached
  path, else Who-Is-Router-To-Network). forget_route drops that cached path
  so a router that moved or was replaced is found again instead of being
  asked forever.
"""

from __future__ import annotations

import asyncio
import os

from windrow import legacy

# What the BBMD registration status codes from bacpypes3's BIPForeign mean.
_REGISTRATION = {-2: "unregistered", -1: "registering", 0: "registered"}


def add_network_args(parser) -> None:
    """Give bacpypes3's BBMD/foreign-device/network options Windrow env-var
    defaults (the command line still wins). The options themselves come from
    SimpleArgumentParser:
      --foreign ADDR   register as a foreign device with the BBMD at ADDR
      --ttl SECONDS    registration time-to-live (renewed at that interval)
      --bbmd [ADDR...] act as a BBMD with this broadcast distribution table
      --network N      this port's BACnet network number
    """
    defaults = {}
    foreign = legacy.env("WINDROW_BACNET_FOREIGN_BBMD")
    if foreign:
        defaults["foreign"] = foreign
    ttl = legacy.env("WINDROW_BACNET_FOREIGN_TTL")
    if ttl:
        defaults["ttl"] = int(ttl)
    bdt = os.environ.get("WINDROW_BACNET_BBMD_BDT")  # not legacy.env: set-but-empty means something here
    if bdt is not None:
        # space or comma separated; set but empty = a BBMD with no peers
        defaults["bbmd"] = [peer for peer in bdt.replace(",", " ").split() if peer]
    network = legacy.env("WINDROW_BACNET_NETWORK")
    if network:
        defaults["network"] = int(network)
    if defaults:
        parser.set_defaults(**defaults)


def parse_device_address(value) -> dict:
    """A device address as a connection stores it, checked: a local station
    (`10.0.0.5`, `10.0.0.5:47809`) or a routed one (`net:mac`). Returns
    {address, network, mac}; ValueError for a broadcast or anything
    bacpypes3 can't parse, so a typo is a 400 rather than a poll loop that
    never reads."""
    from bacpypes3.pdu import Address

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"device_address must be a non-empty string, got {value!r}")
    try:
        address = Address(value.strip())
    except Exception as exc:  # bacpypes3 raises a mix of ValueError/TypeError/its own
        raise ValueError(f"device_address {value!r} isn't a BACnet address: {exc}") from exc
    if address.addrType == Address.localStationAddr:
        return {"address": str(address), "network": None, "mac": str(address)}
    if address.addrType == Address.remoteStationAddr:
        text = str(address)
        return {"address": text, "network": address.addrNet, "mac": text.split(":", 1)[1].split("@", 1)[0]}
    raise ValueError(f"device_address {value!r} is a broadcast address, not a device")


def address_key(device_address: str) -> str:
    """The part of a device address that identifies the device: the host of
    a local BACnet/IP address (a connection often carries :47808, an I-Am's
    source doesn't), or the whole of a routed `net:mac` one - splitting that
    on ':' too would make every device on network 5 the same device "5"."""
    from bacpypes3.pdu import Address

    try:
        address = Address(device_address)
    except Exception:  # noqa: BLE001 - not parseable: compare it as given, minus any port
        return str(device_address).split(":")[0]
    if address.addrType == Address.remoteStationAddr:
        return str(address)
    tuple_ = getattr(address, "addrTuple", None)
    return tuple_[0] if tuple_ else str(address)


def describe_source(address) -> dict:
    """{address, network, mac} for an I-Am's source address."""
    from bacpypes3.pdu import Address

    text = str(address)
    if getattr(address, "addrType", None) == Address.remoteStationAddr:
        return {"address": text, "network": address.addrNet, "mac": text.split(":", 1)[1].split("@", 1)[0]}
    return {"address": text, "network": None, "mac": text}


def forget_route(app, device_address: str) -> bool:
    """Drop the cached router path to a routed device's network, so the next
    request looks for the router again (Who-Is-Router-To-Network). Called
    by the poller after repeated failures. False for a local device, or when
    nothing was cached."""
    from bacpypes3.pdu import Address

    try:
        address = Address(device_address)
        cache = app.nsap.router_info_cache
    except Exception:  # noqa: BLE001 - no app (tests), or not an address: nothing to forget
        return False
    if address.addrType != Address.remoteStationAddr:
        return False
    dropped = False
    for (snet, dnet) in list(cache.path_info):
        if dnet == address.addrNet:
            del cache.path_info[(snet, dnet)]
            dropped = True
    for key, dnets in list(cache.router_dnets.items()):
        dnets.discard(address.addrNet)
    return dropped


async def discover_routers(app, network: int | None = None, timeout: float = 5.0) -> list[dict]:
    """Who-Is-Router-To-Network (for one network, or all): every router that
    answers and the networks it says it reaches."""
    try:
        answers = await asyncio.wait_for(app.nse.who_is_router_to_network(network=network), timeout)
    except (TimeoutError, asyncio.TimeoutError):
        return []
    routers: dict[str, set[int]] = {}
    for _adapter, npdu in answers or []:
        routers.setdefault(str(npdu.pduSource), set()).update(int(n) for n in npdu.iartnNetworkList)
    return [{"address": address, "networks": sorted(nets)} for address, nets in sorted(routers.items())]


def network_status(app) -> dict:
    """What the BACnet/IP link is doing: its mode, and for a foreign device
    whether the BBMD has accepted the registration; for a BBMD, its
    distribution and foreign-device tables; and the routes learned so far
    (network -> router)."""
    from bacpypes3.ipv4.service import BIPBBMD, BIPForeign

    status: dict = {"mode": "normal", "routes": []}
    try:
        link_layers = list(app.link_layers.values())
        nsap = app.nsap
    except AttributeError:
        return status
    for link in link_layers:
        if isinstance(link, BIPForeign):
            code = link.bbmdRegistrationStatus
            status.update({
                "mode": "foreign",
                "bbmd": str(link.bbmdAddress) if link.bbmdAddress else None,
                "ttl": link.bbmdTimeToLive,
                "registration": _REGISTRATION.get(code, f"rejected ({code})"),
            })
        elif isinstance(link, BIPBBMD):
            status.update({
                "mode": "bbmd",
                "bdt": [str(peer) for peer in link.bbmdBDT],
                "foreign_devices": [
                    {"address": str(fd.fdAddress), "ttl": fd.fdTTL, "remaining": fd.fdRemain} for fd in link.bbmdFDT
                ],
            })
    status["networks"] = sorted(n for n in nsap.adapters if n is not None) if nsap.adapters else []
    status["routes"] = sorted(
        ({"network": dnet, "router": str(router)} for (_snet, dnet), (router, _status) in nsap.router_info_cache.path_info.items()),
        key=lambda r: r["network"],
    )
    return status

