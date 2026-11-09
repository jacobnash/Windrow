"""
BACnet value normalization, shared by every BACnet-facing bridge (the
static CLI bridge and the dynamic connection manager alike) - one place to
get this right instead of two.
"""

from bacpypes3.primitivedata import Enumerated


def normalize_value(value):
    """BACnet enumerated types (e.g. active/inactive) subclass int, so
    json.dumps silently serializes them as raw ints instead of erroring.
    Check Enumerated before the int/float branch or it's shadowed."""
    if isinstance(value, Enumerated):
        return str(value)
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


# StatusFlags bit order (ASHRAE 135, clause 12): bit 0 is in-alarm.
STATUS_FLAG_NAMES = ("in-alarm", "fault", "overridden", "out-of-service")


def quality_of(status_flags=None, reliability=None) -> dict | None:
    """A reading's `quality` field (Windrow#20), from the object's
    status-flags and reliability as the device reports them - raw, not
    judged: Baler's quality.from_envelope decides what counts as bad
    (in-alarm alone doesn't). None when there's nothing to say - every flag
    clear and reliability no-fault-detected or not read - so a healthy
    point's envelope is exactly what it was before.

    status_flags: a bacpypes3 StatusFlags (a list of 0/1 bits) or anything
    iterable the same way. reliability: a bacpypes3 Reliability or its name."""
    quality: dict = {}
    if status_flags is not None:
        try:
            bits = list(status_flags)
        except TypeError:
            bits = []
        flags = [name for name, bit in zip(STATUS_FLAG_NAMES, bits) if bit]
        if flags:
            quality["status_flags"] = flags
    if reliability is not None:
        name = str(reliability)
        if name and name.replace("-", "").lower() != "nofaultdetected":
            quality["reliability"] = name
    return quality or None
