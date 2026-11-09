"""
BACnet device/point discovery - the "Discover" and "Learn" halves of the
SkySpark-style flow (point at a network, find devices; point at a device,
find its points), so nothing downstream ever needs a hand-typed object
identifier. Real BACnet client calls via bacpypes3's Application, no
protocol code of our own.
"""

import asyncio
import os

from bacpypes3.apdu import AbortPDU, ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.basetypes import DeviceObjectReference, ErrorType
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Real

from windrow import bacnet_network, tracing
from windrow.bacnet_values import normalize_value

tracer = tracing.get_tracer(__name__)

# The object-list read can be long (a segmented answer from a big
# controller), so more time than a single point read; but bounded (Baler#84).
LEARN_TIMEOUT = float(os.environ.get("WINDROW_LEARN_TIMEOUT", "60"))

LEARN_PROPERTIES = ["object-name", "present-value", "units", "description"]
# Asked on top of LEARN_PROPERTIES, by object type (Windrow#19): the raw
# evidence a bad name leaves - range, COV increment, state texts, and
# whether the object is commandable. relinquish-default stands in for
# priority-array: the standard requires one exactly when the other is
# present, and it is one value where the array is sixteen.
_ANALOG = ["min-pres-value", "max-pres-value", "cov-increment"]
_BINARY = ["active-text", "inactive-text"]
_MULTISTATE = ["state-text"]
TYPE_PROPERTIES = {
    "analog-input": _ANALOG,
    "analog-output": [*_ANALOG, "relinquish-default"],
    "analog-value": [*_ANALOG, "relinquish-default"],
    "binary-input": _BINARY,
    "binary-output": [*_BINARY, "relinquish-default"],
    "binary-value": [*_BINARY, "relinquish-default"],
    "multi-state-input": _MULTISTATE,
    "multi-state-output": [*_MULTISTATE, "relinquish-default"],
    "multi-state-value": [*_MULTISTATE, "relinquish-default"],
}
# Object types whose commandability is worth asking (the ones above that
# ask relinquish-default, plus any that might).
_COMMANDABLE_ASKED = {t for t, props in TYPE_PROPERTIES.items() if "relinquish-default" in props}
# A structured view is the integrator's own equipment grouping, when they
# built one: not a point, but its subordinate-list says which points belong
# together.
STRUCTURED_VIEW = "structured-view"
VIEW_PROPERTIES = ["object-name", "subordinate-list"]
# The device object's identity, read once per learn, best effort.
DEVICE_PROPERTIES = ["object-name", "vendor-name", "model-name", "location"]
# Objects per ReadPropertyMultiple. One request for a whole big device came
# back "apdu-too-long" (the mock hospital, and any controller without
# segmentation); a batch that's still too long is split in half and retried.
LEARN_BATCH = 16


# What discover_devices() takes. Anything else in a request body - a command
# envelope's expires_at, a caller's bookkeeping - is dropped rather than
# splatted in as a keyword it doesn't know (Baler#140).
DISCOVER_PARAMS = ("low_limit", "high_limit", "address", "timeout")


def discover_params(body: dict) -> dict:
    return {k: body[k] for k in DISCOVER_PARAMS if k in body}


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


def properties_for(obj_id) -> list[str]:
    """What learn asks of one object: LEARN_PROPERTIES and its type's extras,
    or a structured view's name and members."""
    object_type = str(obj_id[0])
    if object_type == STRUCTURED_VIEW:
        return VIEW_PROPERTIES
    return LEARN_PROPERTIES + TYPE_PROPERTIES.get(object_type, [])


def raw_value(value):
    """A property value as plain JSON, uninterpreted: arrays and lists as
    lists (state-text), enumerations and object identifiers as their text,
    a DeviceObjectReference as {object_identifier[, device_identifier]}.
    A read error is None."""
    if value is None or isinstance(value, ErrorType):
        return None
    if isinstance(value, DeviceObjectReference):
        ref = {"object_identifier": str(value.objectIdentifier)}
        if value.deviceIdentifier is not None:
            ref["device_identifier"] = str(value.deviceIdentifier)
        return ref
    if isinstance(value, ObjectIdentifier):
        return str(value)
    if isinstance(value, list):
        return [raw_value(v) for v in value]
    if isinstance(value, Real):
        # a BACnet Real is single precision: 0.1 comes back 0.10000000149011612
        return float(f"{float(value):.7g}")
    return normalize_value(value)


async def _read_batch(app: Application, device_address: str, object_ids: list) -> list:
    """properties_for() each object in one ReadPropertyMultiple, halving
    the batch while the device says the answer won't fit in one APDU."""
    parameter_list = []
    for obj_id in object_ids:
        parameter_list.append(obj_id)
        parameter_list.append(properties_for(obj_id))
    try:
        results = await app.read_property_multiple(device_address, parameter_list)
    except AbortPDU as exc:
        too_big = "too-long" in str(exc) or "segmentation" in str(exc)
        if too_big and len(object_ids) > 1:
            half = len(object_ids) // 2
            return (await _read_batch(app, device_address, object_ids[:half])
                    + await _read_batch(app, device_address, object_ids[half:]))
        raise RuntimeError(f"{device_address} aborted the point read: {exc}") from None
    except ErrorRejectAbortNack as exc:
        # BaseException subclasses in bacpypes3: make them ordinary errors
        raise RuntimeError(f"{device_address} refused the point read: {exc}") from None
    if not isinstance(results, list):
        raise RuntimeError(f"read_property_multiple failed for {device_address}: {results!r}")
    return results


async def learn_points(app: Application, device_address: str, device_instance: int) -> list[dict]:
    """Reads a device's object-list, then batches a read of
    object-name/present-value/units per object - the "here's what's on
    this device" candidate list a caller picks points from. Per-property
    read failures (bacpypes3 embeds an ErrorType in the result tuple
    rather than raising) are reported as None fields, not a crashed call -
    a device that can't report e.g. units for one point shouldn't hide
    every other point on it.

    Each point also carries the raw evidence a bad name leaves (Windrow#19),
    uninterpreted: `object_type`; `commandable` (it has a priority array)
    for the types that can be; `state_text` / `active_text` /
    `inactive_text`; `min_pres_value`, `max_pres_value`, `cov_increment`;
    `structured_views`, the names of the integrator's structured-view
    objects that list it; and `device`, the device object's name, vendor,
    model and location. A property the object doesn't have is left out.
    Structured views themselves aren't points and aren't returned."""
    with tracer.start_as_current_span("discovery.learn_points") as span:
        span.set_attribute("device_address", device_address)
        span.set_attribute("device_instance", device_instance)

        # object-list comes back as plain (ObjectType, instance) tuples, not
        # ObjectIdentifier instances - read_property_multiple requires the
        # latter (raises TypeError("objid") otherwise, confirmed live).
        # ObjectType also doesn't compare equal to a plain str directly
        # (`obj_id[0] == "device"` is always False, confirmed live) - str()
        # it first.
        try:
            object_list = await asyncio.wait_for(
                app.read_property(device_address, f"device,{device_instance}", "object-list"), LEARN_TIMEOUT
            )
        except ErrorRejectAbortNack as exc:
            # a BaseException in bacpypes3 - an ordinary error here, like _read_batch's
            raise RuntimeError(f"{device_address} refused reading device,{device_instance}'s object-list: {exc}") from None
        object_ids = [ObjectIdentifier(obj_id) for obj_id in object_list if str(obj_id[0]) != "device"]

        if not object_ids:
            span.set_attribute("point_count", 0)
            return []

        # Flat, not a list of (objid, props) pairs - read_property_multiple
        # unpacks it two-at-a-time internally
        # (`objid, props, *rest = parameter_list`), confirmed live: passing
        # pairs makes `objid` bind to an entire (objid, props) tuple on the
        # first iteration and raise TypeError("objid").
        results = []
        for start in range(0, len(object_ids), LEARN_BATCH):
            results += await _read_batch(app, device_address, object_ids[start : start + LEARN_BATCH])
        span.set_attribute("batch_size", LEARN_BATCH)

        by_object: dict[str, dict] = {str(obj_id): {} for obj_id in object_ids}
        for obj_id, prop_id, _array_index, value in results:
            field = str(prop_id).replace("-", "_")
            by_object.setdefault(str(obj_id), {})[field] = value

        device = await read_device_identity(app, device_address, device_instance)
        views = _structured_views(object_ids, by_object)

        points = []
        for obj_id in object_ids:
            if str(obj_id[0]) == STRUCTURED_VIEW:
                continue
            props = by_object[str(obj_id)]
            point = {
                "object_identifier": str(obj_id),
                "label": raw_value(props.get("object_name")) or str(obj_id),
                "present_value": raw_value(props.get("present_value")),
                "units": raw_value(props.get("units")),
                "description": raw_value(props.get("description")),
                "object_type": str(obj_id[0]),
            }
            if str(obj_id[0]) in _COMMANDABLE_ASKED:
                # an answer (even Null) means the property exists; an error means it doesn't
                answer = props.get("relinquish_default")
                point["commandable"] = answer is not None and not isinstance(answer, ErrorType)
            for field in ("state_text", "active_text", "inactive_text", "min_pres_value", "max_pres_value", "cov_increment"):
                value = raw_value(props.get(field))
                if value is not None:
                    point[field] = value
            if str(obj_id) in views:
                point["structured_views"] = views[str(obj_id)]
            if device:
                point["device"] = device
            points.append(point)
        span.set_attribute("point_count", len(points))
        return points


async def read_device_identity(app: Application, device_address: str, device_instance: int) -> dict:
    """DEVICE_PROPERTIES of the device object, as {"instance", "name",
    "vendor_name", "model_name", "location"} with only what the device
    answered. Best effort: a device that won't answer this still has its
    points learned, so any failure is just fewer keys."""
    device_id = ObjectIdentifier(f"device,{device_instance}")
    identity: dict = {"instance": device_instance}
    try:
        results = await asyncio.wait_for(
            app.read_property_multiple(device_address, [device_id, DEVICE_PROPERTIES]), LEARN_TIMEOUT
        )
    except (Exception, ErrorRejectAbortNack) as exc:  # noqa: BLE001 - identity is extra evidence, not the learn
        print(f"{device_address}: couldn't read device,{device_instance}'s identity: {exc!r}")
        return identity
    if not isinstance(results, list):
        return identity
    for _obj_id, prop_id, _array_index, value in results:
        value = raw_value(value)
        if value is None or value == "":
            continue
        field = str(prop_id).replace("-", "_")
        identity["name" if field == "object_name" else field] = value
    return identity


def _structured_views(object_ids: list, by_object: dict) -> dict[str, list[str]]:
    """{point object id: [names of the structured views listing it]}, for
    members on this device (a reference without a device identifier, or
    naming no other device). A view's name falls back to its identifier."""
    member_of: dict[str, list[str]] = {}
    for obj_id in object_ids:
        if str(obj_id[0]) != STRUCTURED_VIEW:
            continue
        props = by_object.get(str(obj_id), {})
        name = raw_value(props.get("object_name")) or str(obj_id)
        for ref in raw_value(props.get("subordinate_list")) or []:
            if not isinstance(ref, dict):
                continue
            member = ref["object_identifier"]
            member_of.setdefault(member, [])
            if name not in member_of[member]:
                member_of[member].append(name)
    return member_of
