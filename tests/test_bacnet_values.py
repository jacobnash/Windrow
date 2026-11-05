from bacpypes3.basetypes import BinaryPV

from windrow.bacnet_values import normalize_value


def test_enumerated_becomes_string_not_raw_int():
    # BinaryPV subclasses int - this is the exact bug that cost hours in
    # the Haxall integration (silent int serialization instead of "active")
    assert normalize_value(BinaryPV("active")) == "active"
    assert isinstance(normalize_value(BinaryPV("active")), str)


def test_plain_types_pass_through_unchanged():
    assert normalize_value(71.0) == 71.0
    assert normalize_value(71) == 71
    assert normalize_value("active") == "active"
    assert normalize_value(True) is True
    assert normalize_value(None) is None


def test_unknown_type_falls_back_to_str():
    class Weird:
        def __str__(self):
            return "weird-value"

    assert normalize_value(Weird()) == "weird-value"
