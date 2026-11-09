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


def priority_levels(priority_array) -> dict | None:
    """A commandable object's priority-array as {"active": level or None,
    "levels": {"<level>": value}} (Windrow#25): only the levels someone
    holds, keyed by level number as text (JSON object keys), values
    normalized like present-value. `active` is the lowest-numbered held
    level - the one in control - or None when every level is empty and
    relinquish-default is in control. None for anything that isn't a
    16-slot array."""
    try:
        slots = list(priority_array)
    except TypeError:
        return None
    if len(slots) != 16:
        return None
    levels: dict = {}
    for level, slot in enumerate(slots, start=1):
        choice = getattr(slot, "_choice", None)
        if choice is None or choice == "null":
            continue
        value = getattr(slot, choice, None)
        if value is None:
            continue
        levels[str(level)] = normalize_value(value)
    active = min((int(k) for k in levels), default=None)
    return {"active": active, "levels": levels}
