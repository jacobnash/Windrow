"""
Requires the live windrow-haxall container plus the mock devices/bridges
already publishing to mosquitto. Running provision_haxall's steps here
is itself the idempotency proof - the script is designed to be safe to
rerun, and this test relies on that being true.
"""

import pytest

from windrow.haxall_client import connect, eval_axon
from windrow.provision_haxall import (
    ensure_equip,
    ensure_libs,
    ensure_point,
    ensure_point_tags,
    ensure_site,
    ensure_task,
    find_ref,
)

HAXALL_URI = "http://localhost:8180/"
HAXALL_USER = "su"
HAXALL_PASS = "windrow-demo-pass"


@pytest.fixture
def session():
    return connect(HAXALL_URI, HAXALL_USER, HAXALL_PASS)


@pytest.mark.integration
def test_provisioning_is_idempotent_and_all_tasks_healthy(session):
    ensure_libs(session)
    conn_ref = find_ref(session, "mqttConn")
    assert conn_ref is not None

    bacnet_fan_ref = find_ref(session, 'point and dis=="Fan Status"')
    modbus_temp_ref = ensure_point(session, "Modbus Zone Temp", "Number", unit="°F")
    modbus_fan_ref = ensure_point(session, "Modbus Fan Status", "Bool")

    ensure_task(session, "Windrow Ingest BACnet Fan Status", conn_ref, "fbf/mock-ahu-1/binaryValue,1", bacnet_fan_ref, 'json->value == "active"')
    ensure_task(session, "Windrow Ingest Modbus Zone Temp", conn_ref, "fbf/mock-modbus-1/zone-temp", modbus_temp_ref, "json->value")
    ensure_task(session, "Windrow Ingest Modbus Fan Status", conn_ref, "fbf/mock-modbus-1/fan-status", modbus_fan_ref, "json->value == 1")

    tasks = eval_axon(session, "tasks()")
    expected = {
        "Windrow Ingest Zone Temp",
        "Windrow Ingest BACnet Fan Status",
        "Windrow Ingest Modbus Zone Temp",
        "Windrow Ingest Modbus Fan Status",
    }
    seen = {row["id"].value for row in tasks}
    assert expected.issubset(seen)

    for row in tasks:
        if row["id"].value in expected:
            assert row["errNum"] == 0.0, f"{row['id'].value} has errors: {row}"
            assert row["evalNum"] > 0, f"{row['id'].value} never evaluated"


@pytest.mark.integration
def test_all_four_points_have_live_typed_values(session):
    points = eval_axon(session, "readAll(point)")
    by_dis = {row["dis"]: row for row in points}

    assert {"Zone Temp", "Fan Status", "Modbus Zone Temp", "Modbus Fan Status"}.issubset(by_dis)
    assert by_dis["Zone Temp"]["curStatus"] == "ok"
    assert isinstance(by_dis["Zone Temp"]["curVal"], float)
    assert isinstance(by_dis["Fan Status"]["curVal"], bool)
    assert isinstance(by_dis["Modbus Zone Temp"]["curVal"], float)
    assert isinstance(by_dis["Modbus Fan Status"]["curVal"], bool)


@pytest.mark.integration
def test_tagging_model_is_idempotent_and_filterable(session):
    site_ref = ensure_site(session, "Windrow Demo Site")
    ahu1_ref = ensure_equip(session, "AHU-1", site_ref, markers=["ahu"])
    ahu2_ref = ensure_equip(session, "AHU-2", site_ref, markers=["ahu"])

    bacnet_temp_ref = find_ref(session, 'point and dis=="Zone Temp"')
    bacnet_fan_ref = find_ref(session, 'point and dis=="Fan Status"')
    modbus_temp_ref = find_ref(session, 'point and dis=="Modbus Zone Temp"')
    modbus_fan_ref = find_ref(session, 'point and dis=="Modbus Fan Status"')
    ensure_point_tags(session, bacnet_temp_ref, ["zone", "air", "temp", "sensor"], ahu1_ref)
    ensure_point_tags(session, bacnet_fan_ref, ["fan", "run", "sensor"], ahu1_ref)
    ensure_point_tags(session, modbus_temp_ref, ["zone", "air", "temp", "sensor"], ahu2_ref)
    ensure_point_tags(session, modbus_fan_ref, ["fan", "run", "sensor"], ahu2_ref)
    ensure_point(session, "Windrow Write Demo", "Number", cur=False)

    assert len(eval_axon(session, "readAll(ahu)")) == 2
    assert len(eval_axon(session, "readAll(zone and air and temp and sensor)")) == 2
    assert len(eval_axon(session, "readAll(fan and run and sensor)")) == 2
    assert len(eval_axon(session, f"readAll(point and equipRef->siteRef==@{site_ref})")) >= 4

    write_demo = eval_axon(session, 'readAll(point and dis=="Windrow Write Demo")')
    assert len(write_demo) == 1
    assert write_demo[0].get("writable") is not None
    assert write_demo[0].get("equipRef") is None
