from windrow.modbus_bridge import normalize_value, parse_points


def test_single_register_passthrough():
    assert normalize_value([71]) == 71


def test_empty_registers_is_none():
    assert normalize_value([]) is None
    assert normalize_value(None) is None


def test_parse_points():
    assert parse_points(["zone-temp:0", "fan-status:1"]) == {"zone-temp": 0, "fan-status": 1}
