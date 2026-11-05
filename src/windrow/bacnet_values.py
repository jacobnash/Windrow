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
