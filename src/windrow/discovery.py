"""
BACnet device/point discovery - the "Discover" and "Learn" halves of the
SkySpark-style flow (point at a network, find devices; point at a device,
find its points), so nothing downstream ever needs a hand-typed object
identifier. Real BACnet client calls via bacpypes3's Application, no
protocol code of our own.
"""

from bacpypes3.app import Application
from bacpypes3.basetypes import ErrorType
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier

from windrow import bacnet_network, tracing
from windrow.bacnet_values import normalize_value

tracer = tracing.get_tracer(__name__)

LEARN_PROPERTIES = ["object-name", "present-value", "units", "description"]


async def discover_devices(
    app: Application,
    low_limit: int | None = None,
    high_limit: int | None = None,
    address: str | None = None,
    timeout: float | None = None,
    network: int | None = None,
    global_broadcast: bool = True,
) -> list[dict]:
    """Broadcasts Who-Is and returns every I-Am response as a plain dict -
    the device picker's data source.

    Where the Who-Is goes (Baler#126): `address` targets one device (a
    local `ip[:port]` or a routed `net:mac`); `network` broadcasts on that
    one remote network (`net:*`); otherwise a global broadcast (`*:*`),
    which every router forwards to the networks behind it - MS/TP trunks
    included - and which a foreign device hands its BBMD to distribute.
    `global_broadcast=False` keeps it to this subnet. A device behind a
    router answers with a `net:mac` address, which is what a connection to
    it uses."""
    with tracer.start_as_current_span("discovery.discover_devices") as span:
        kwargs = {}
        if low_limit is not None:
            kwargs["low_limit"] = low_limit
        if high_limit is not None:
            kwargs["high_limit"] = high_limit
        if address is not None:
            # who_is() needs an Address object - a raw string fails deep
            # inside with a confusing AttributeError (is_localstation),
            # confirmed live against the real mock device.
            kwargs["address"] = address if isinstance(address, Address) else Address(address)
        elif network is not None:
            if isinstance(network, bool) or not isinstance(network, int) or not 1 <= network <= 65534:
                raise ValueError(f"network must be a BACnet network number 1-65534, got {network!r}")
            kwargs["address"] = Address(f"{network}:*")
        elif global_broadcast:
            kwargs["address"] = Address("*:*")
        span.set_attribute("who_is_to", str(kwargs.get("address", "local broadcast")))
        if timeout is not None:
            kwargs["timeout"] = timeout

        i_ams = await app.who_is(**kwargs)

        devices = []
        seen = set()
        for i_am in i_ams:
            source = bacnet_network.describe_source(i_am.pduSource)
            key = (i_am.iAmDeviceIdentifier[1], source["address"])
            if key in seen:  # a global broadcast can reach a device by two paths
                continue
            seen.add(key)
            devices.append({
                "device_instance": i_am.iAmDeviceIdentifier[1],
                "address": source["address"],
                "network": source["network"],
                "mac": source["mac"],
                "vendor_id": int(i_am.vendorID),
                "max_apdu": int(i_am.maxAPDULengthAccepted),
            })
        span.set_attribute("device_count", len(devices))
        return devices


async def learn_points(app: Application, device_address: str, device_instance: int) -> list[dict]:
    """Reads a device's object-list, then batches a read of
    object-name/present-value/units per object - the "here's what's on
    this device" candidate list a caller picks points from. Per-property
    read failures (bacpypes3 embeds an ErrorType in the result tuple
    rather than raising) are reported as None fields, not a crashed call -
    a device that can't report e.g. units for one point shouldn't hide
    every other point on it."""
    with tracer.start_as_current_span("discovery.learn_points") as span:
        span.set_attribute("device_address", device_address)
        span.set_attribute("device_instance", device_instance)

        # object-list comes back as plain (ObjectType, instance) tuples, not
        # ObjectIdentifier instances - read_property_multiple requires the
        # latter (raises TypeError("objid") otherwise, confirmed live).
        # ObjectType also doesn't compare equal to a plain str directly
        # (`obj_id[0] == "device"` is always False, confirmed live) - str()
        # it first.
        object_list = await app.read_property(device_address, f"device,{device_instance}", "object-list")
        object_ids = [ObjectIdentifier(obj_id) for obj_id in object_list if str(obj_id[0]) != "device"]

        if not object_ids:
            span.set_attribute("point_count", 0)
            return []

        # Flat, not a list of (objid, props) pairs - read_property_multiple
        # unpacks it two-at-a-time internally
        # (`objid, props, *rest = parameter_list`), confirmed live: passing
        # pairs makes `objid` bind to an entire (objid, props) tuple on the
        # first iteration and raise TypeError("objid").
        parameter_list = []
        for obj_id in object_ids:
            parameter_list.append(obj_id)
            parameter_list.append(LEARN_PROPERTIES)
        results = await app.read_property_multiple(device_address, parameter_list)
        if not isinstance(results, list):
            raise RuntimeError(f"read_property_multiple failed for {device_address}: {results!r}")

        by_object: dict[str, dict] = {str(obj_id): {} for obj_id in object_ids}
        for obj_id, prop_id, _array_index, value in results:
            field = str(prop_id).replace("-", "_")
            by_object[str(obj_id)][field] = None if isinstance(value, ErrorType) else normalize_value(value)

        points = [
            {
                "object_identifier": str(obj_id),
                "label": by_object[str(obj_id)].get("object_name") or str(obj_id),
                "present_value": by_object[str(obj_id)].get("present_value"),
                "units": by_object[str(obj_id)].get("units"),
                "description": by_object[str(obj_id)].get("description"),
            }
            for obj_id in object_ids
        ]
        span.set_attribute("point_count", len(points))
        return points
