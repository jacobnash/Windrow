"""Baler#128: Modbus point specs - strict validation, and decode/encode of
each data type in each word and byte order, scale and offset. Pure
functions, no device."""

import struct

import pytest

from windrow import modbus_points as mp


def _words(fmt: str, value, word_order="big", byte_order="big") -> list[int]:
    """Independent reference encoder: struct big-endian bytes, then swap."""
    data = struct.pack(">" + fmt, value)
    if byte_order == "little":
        data = b"".join(data[i + 1:i + 2] + data[i:i + 1] for i in range(0, len(data), 2))
    words = [int.from_bytes(data[i:i + 2], "big") for i in range(0, len(data), 2)]
    return words[::-1] if word_order == "little" else words


def test_bare_address_is_the_old_holding_uint16_form():
    assert mp.validate_point("t", 7) == 7
    point = mp.resolve(7)
    assert point == {**mp.DEFAULTS, "register": 7, "count": 1}
    assert mp.decode(point, [65535]) == 65535  # unsigned, as normalize_value always published
    assert mp.resolve({"register": 7}) == point


@pytest.mark.parametrize("type_, fmt, value", [
    ("int16", "h", -1234),
    ("uint16", "H", 54321),
    ("int32", "i", -123456789),
    ("uint32", "I", 3000000000),
    ("float32", "f", 55.5),
])
@pytest.mark.parametrize("word_order", ["big", "little"])
@pytest.mark.parametrize("byte_order", ["big", "little"])
def test_each_type_and_order_round_trips(type_, fmt, value, word_order, byte_order):
    spec = {"register": 0, "type": type_, "byte_order": byte_order}
    if fmt.lower() in "if":  # 32-bit
        spec["word_order"] = word_order
    elif word_order == "little":
        pytest.skip("word_order is 32-bit only")
    point = mp.resolve(mp.validate_point("p", spec))
    words = _words(fmt, value, word_order if "word_order" in spec else "big", byte_order)
    assert mp.decode(point, words) == value
    assert mp.encode(point, value) == words


def test_word_order_matters():
    big = mp.resolve({"register": 0, "type": "float32"})
    little = mp.resolve({"register": 0, "type": "float32", "word_order": "little"})
    words = [0x425E, 0x0000]  # 55.5, high word first
    assert mp.decode(big, words) == 55.5
    assert mp.decode(little, words[::-1]) == 55.5
    assert mp.decode(little, words) != 55.5


def test_byte_order_swaps_within_each_register():
    point = mp.resolve({"register": 0, "type": "int16", "byte_order": "little"})
    assert mp.decode(point, [0x3412]) == 0x1234


def test_scale_and_offset():
    tenths = mp.resolve({"register": 0, "type": "int16", "scale": 0.1})
    assert mp.decode(tenths, [723]) == 72.3  # not 72.30000000000001
    assert mp.decode(tenths, [65536 - 45]) == -4.5
    assert mp.encode(tenths, 72.3) == [723]
    assert mp.encode(tenths, 72.34) == [723]  # nearest step the register holds

    c_to_f = mp.resolve({"register": 0, "type": "float32", "scale": 1.8, "offset": 32})
    assert mp.decode(c_to_f, _words("f", 12.8)) == 55.04
    assert mp.encode(c_to_f, 55.04) == _words("f", 12.8)


def test_float32_publishes_its_own_precision_and_nan_publishes_nothing():
    point = mp.resolve({"register": 0, "type": "float32"})
    assert mp.decode(point, _words("f", 21.3)) == 21.3  # not 21.299999237060547
    assert mp.decode(point, _words("f", float("nan"))) is None


def test_bits():
    coil = mp.resolve({"register": 3, "function": "coil", "writable": True})
    assert coil["count"] == 1
    assert mp.decode(coil, [True, False]) is True
    assert mp.encode(coil, False) is False
    assert mp.encode(coil, 1) is True
    with pytest.raises(ValueError):
        mp.encode(coil, 2)
    with pytest.raises(ValueError):
        mp.encode(coil, "on")


@pytest.mark.parametrize("spec, value", [
    ({"register": 0, "type": "int16"}, 40000),
    ({"register": 0, "type": "uint16"}, -1),
    ({"register": 0, "type": "uint32"}, 2**32),
    ({"register": 0, "type": "int16", "scale": 0.1}, 4000),  # raw 40000
    ({"register": 0, "type": "float32"}, 1e39),
    ({"register": 0}, True),
    ({"register": 0}, "72"),
    ({"register": 0}, float("inf")),
])
def test_encode_refuses_what_wont_fit(spec, value):
    with pytest.raises(ValueError):
        mp.encode(mp.resolve(spec), value)


@pytest.mark.parametrize("spec, msg", [
    (True, "integer"),
    (-1, "0-65535"),
    (65536, "0-65535"),
    ("40001", "register address or a point object"),
    ({}, "register is required"),
    ({"register": "1"}, "integer 0-65535"),
    ({"register": 1, "regsiter": 2}, "unknown field"),
    ({"register": 1, "function": "holdings"}, "function must be"),
    ({"register": 1, "type": "float64"}, "type must be"),
    ({"register": 65535, "type": "float32"}, "runs past"),
    ({"register": 1, "type": "int32", "word_order": "middle"}, "word_order"),
    ({"register": 1, "byte_order": 1}, "byte_order"),
    ({"register": 1, "word_order": "little"}, "32-bit"),
    ({"register": 1, "scale": 0}, "can't be 0"),
    ({"register": 1, "scale": "0.1"}, "finite number"),
    ({"register": 1, "offset": float("nan")}, "finite number"),
    ({"register": 1, "unit": ""}, "unit"),
    ({"register": 1, "writable": "yes"}, "true or false"),
    ({"register": 1, "function": "input", "writable": True}, "only holding registers and coils"),
    ({"register": 1, "function": "discrete", "writable": True}, "only holding registers and coils"),
    ({"register": 1, "function": "coil", "type": "int16"}, "don't apply"),
    ({"register": 1, "function": "coil", "scale": 2}, "don't apply"),
])
def test_validation_is_strict(spec, msg):
    with pytest.raises(ValueError, match=msg):
        mp.validate_point("p", spec)


def test_valid_object_is_stored_as_given():
    spec = {"register": 2, "function": "input", "type": "float32", "word_order": "little", "unit": " °C "}
    assert mp.validate_point("p", spec) == {**spec, "unit": "°C"}
