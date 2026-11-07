"""
What one Modbus point is: which table it lives in, how many registers it
spans, how those registers decode to a number, and how a number encodes back
(Baler#128).

A connection's `points` maps each label to either

- a bare integer, the original form: a holding register read as one
  unsigned 16-bit word, published as-is. Stored connections all use it, so
  it stays valid and means exactly what it always meant; or
- an object:

    {"register": 100,            # required, 0-based protocol address
     "function": "holding",      # holding | input | coil | discrete
     "type": "float32",          # int16 | uint16 | int32 | uint32 | float32
     "word_order": "big",        # 32-bit types: "big" = high word first
     "byte_order": "big",        # within each register: "big" = high byte first
     "scale": 0.1, "offset": 0,  # published value = raw * scale + offset
     "unit": "°F",               # Haystack unit (or use the connection's `units`)
     "writable": true}           # holding registers and coils only

`register` is the 0-based address on the wire, not the 1-based "Modicon"
number some vendor sheets print: holding register 40001 (or 400001) is
`{"function": "holding", "register": 0}`, input register 30010 is
`{"function": "input", "register": 9}`, coil 00001 is `{"function": "coil",
"register": 0}`.

Every key but `register` is optional, so `{"register": 5}` reads exactly
like the bare `5`. Coils and discrete inputs are single bits and publish
true/false; type, word/byte order and scale/offset don't apply to them and
are refused rather than ignored.
"""

import math
import struct

FUNCTIONS = ("holding", "input", "coil", "discrete")
BIT_FUNCTIONS = ("coil", "discrete")
WRITABLE_FUNCTIONS = ("holding", "coil")
ORDERS = ("big", "little")

# type -> (registers it spans, struct code)
TYPES = {
    "int16": (1, "h"),
    "uint16": (1, "H"),
    "int32": (2, "i"),
    "uint32": (2, "I"),
    "float32": (2, "f"),
}

FIELDS = {"register", "function", "type", "word_order", "byte_order", "scale", "offset", "unit", "writable"}
REGISTER_ONLY = ("type", "word_order", "byte_order", "scale", "offset")

DEFAULTS = {
    "function": "holding",
    "type": "uint16",
    "word_order": "big",
    "byte_order": "big",
    "scale": 1,
    "offset": 0,
    "writable": False,
}


def _finite(name: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number, got {value!r}")
    return value


def validate_point(label: str, spec):
    """A bare register address or a point object, checked strictly: an
    unknown key, a wrong type, or an option that can't apply to the
    function is a ValueError (a 400 / ok:false), not a silent default.
    Returns what to store - the bare int unchanged, the object with its
    given keys only (defaults are filled in by resolve())."""
    where = f"point {label!r}"
    if isinstance(spec, bool):
        raise ValueError(f"{where}: register address must be an integer, got {spec!r}")  # noqa: TRY004 - ValueError means 400 / ok:false
    if isinstance(spec, int):
        if not 0 <= spec <= 65535:
            raise ValueError(f"{where}: register address must be 0-65535, got {spec!r}")
        return spec
    if not isinstance(spec, dict):
        raise ValueError(f"{where} must be a register address or a point object, got {spec!r}")  # noqa: TRY004 - ValueError means 400 / ok:false

    unknown = sorted(set(spec) - FIELDS)
    if unknown:
        raise ValueError(f"{where}: unknown field(s) {', '.join(unknown)}; allowed: {', '.join(sorted(FIELDS))}")
    if "register" not in spec:
        raise ValueError(f"{where}: register is required")
    register = spec["register"]
    if isinstance(register, bool) or not isinstance(register, int) or not 0 <= register <= 65535:
        raise ValueError(f"{where}: register must be an integer 0-65535, got {register!r}")

    function = spec.get("function", DEFAULTS["function"])
    if function not in FUNCTIONS:
        raise ValueError(f"{where}: function must be one of {', '.join(FUNCTIONS)}, got {function!r}")
    if function in BIT_FUNCTIONS:
        given = [key for key in REGISTER_ONLY if key in spec]
        if given:
            raise ValueError(f"{where}: {', '.join(given)} don't apply to a {function} (a single bit)")

    type_ = spec.get("type", DEFAULTS["type"])
    if type_ not in TYPES:
        raise ValueError(f"{where}: type must be one of {', '.join(TYPES)}, got {type_!r}")
    count = TYPES[type_][0]
    if register + count - 1 > 65535:
        raise ValueError(f"{where}: a {type_} at register {register} runs past register 65535")
    for key in ("word_order", "byte_order"):
        if key in spec and spec[key] not in ORDERS:
            raise ValueError(f"{where}: {key} must be \"big\" or \"little\", got {spec[key]!r}")
    if "word_order" in spec and count == 1:
        raise ValueError(f"{where}: word_order only applies to 32-bit types, not {type_}")

    if "scale" in spec and _finite(f"{where}: scale", spec["scale"]) == 0:
        raise ValueError(f"{where}: scale can't be 0")
    if "offset" in spec:
        _finite(f"{where}: offset", spec["offset"])
    if "unit" in spec and (not isinstance(spec["unit"], str) or not spec["unit"].strip()):
        raise ValueError(f"{where}: unit must be a non-empty string, got {spec['unit']!r}")
    if "writable" in spec:
        if not isinstance(spec["writable"], bool):
            raise ValueError(f"{where}: writable must be true or false, got {spec['writable']!r}")
        if spec["writable"] and function not in WRITABLE_FUNCTIONS:
            raise ValueError(f"{where}: only holding registers and coils can be written, not a {function}")

    stored = dict(spec)
    if "unit" in stored:
        stored["unit"] = stored["unit"].strip()
    return stored


def resolve(spec) -> dict:
    """Either stored form -> every field filled in, plus `count`."""
    point = {**DEFAULTS, "register": spec} if isinstance(spec, int) else {**DEFAULTS, **spec}
    point["count"] = 1 if point["function"] in BIT_FUNCTIONS else TYPES[point["type"]][0]
    return point


def register_of(spec) -> int:
    return spec if isinstance(spec, int) else spec["register"]


def _to_bytes(registers, point: dict) -> bytes:
    words = list(registers)
    if point["word_order"] == "little":
        words.reverse()
    data = b"".join(word.to_bytes(2, "big") for word in words)
    if point["byte_order"] == "little":
        data = b"".join(data[i + 1:i + 2] + data[i:i + 1] for i in range(0, len(data), 2))
    return data


def _from_bytes(data: bytes, point: dict) -> list[int]:
    if point["byte_order"] == "little":
        data = b"".join(data[i + 1:i + 2] + data[i:i + 1] for i in range(0, len(data), 2))
    words = [int.from_bytes(data[i:i + 2], "big") for i in range(0, len(data), 2)]
    if point["word_order"] == "little":
        words.reverse()
    return words


def _tidy(value: float) -> float:
    # 0.1 * 723 is 72.30000000000001; ten decimals is far finer than any
    # 16/32-bit register can carry, and keeps published values readable.
    return round(value, 10)


def decode(point: dict, words):
    """Registers (or bits) as read -> the published value. Bits publish as
    bool. A register with no scale/offset publishes the raw int (float32
    rounded to its ~7 significant digits); otherwise raw * scale + offset."""
    if point["function"] in BIT_FUNCTIONS:
        return bool(words[0])
    raw = struct.unpack(">" + TYPES[point["type"]][1], _to_bytes(words[: point["count"]], point))[0]
    if point["type"] == "float32":
        if not math.isfinite(raw):
            return None
        raw = float(f"{raw:.7g}")
    if point["scale"] == 1 and point["offset"] == 0:
        return raw
    return _tidy(raw * point["scale"] + point["offset"])


def encode(point: dict, value):
    """A value in engineering units -> what to write: a bool for a coil,
    a list of register words otherwise. Inverse of decode(), so a write of
    72.5 to a scale-0.1 int16 sends 725. Out-of-range or wrong-typed values
    are a ValueError, never wrapped or clipped."""
    if point["function"] in BIT_FUNCTIONS:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        raise ValueError(f"a coil takes true/false (or 1/0), got {value!r}")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"a register takes a finite number, got {value!r}")
    raw = (value - point["offset"]) / point["scale"]
    type_ = point["type"]
    if type_ == "float32":
        if abs(raw) > 3.4028234663852886e38:
            raise ValueError(f"{value!r} is out of range for a float32")
        data = struct.pack(">f", raw)
    else:
        # To the nearest step the register can hold (72.34 at scale 0.1
        # writes 723); the read-back present_value shows what landed.
        raw_int = round(raw)
        try:
            data = struct.pack(">" + TYPES[type_][1], raw_int)
        except struct.error as exc:
            raise ValueError(f"{value!r} (raw {raw_int}) is out of range for a {type_}") from exc
    return _from_bytes(data, point)
