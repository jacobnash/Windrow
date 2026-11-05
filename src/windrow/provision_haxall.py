#!/usr/bin/env python3
"""
Idempotent Haxall provisioning: the 3 missing points/tasks (BACnet Fan
Status task, Modbus Zone Temp point+task, Modbus Fan Status point+task),
reusing the exact recipe from docs/haxall-mqtt-integration.md that already
works for BACnet Zone Temp. Safe to rerun - every step checks-before-commit.
"""

import argparse

from windrow import tracing
from windrow.haxall_client import connect, eval_axon

LIBS = ["ph", "hx.point", "hx.conn", "hx.mqtt", "hx.task"]

tracer = tracing.get_tracer(__name__)


def _step(name: str):
    """One span per provisioning step, error attribute set on failure -
    directly targets the exact failure mode ('a wrong function name
    silently failed for hours') from the earlier hand-typed setup."""
    return tracer.start_as_current_span(f"provision.{name}")


def ensure_libs(session) -> None:
    with _step("ensure_libs") as span:
        try:
            loaded = {row["name"] for row in eval_axon(session, "libs()")}
            for lib in LIBS:
                if lib not in loaded:
                    eval_axon(session, f'libAdd("{lib}")')
                    print(f"  lib added: {lib}")
                else:
                    print(f"  lib already present: {lib}")
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def find_ref(session, filter_expr: str) -> str | None:
    result = eval_axon(session, f"read({filter_expr}, false)")
    row = result[0] if len(result) else None
    if row is None or row.get("id") is None:
        return None
    return row["id"].name  # hszinc Ref -> bare id string


def ensure_point(session, dis: str, kind: str, unit: str | None = None, cur: bool = True) -> str:
    with _step(f"ensure_point:{dis}") as span:
        try:
            existing = find_ref(session, f'point and dis=="{dis}"')
            if existing:
                print(f"  point already exists: {dis}")
                return existing

            unit_tag = f', unit:"{unit}"' if unit else ""
            base_marker = "cur" if cur else "writable"
            eval_axon(session, f'commit(diff(null, {{point, {base_marker}, dis:"{dis}", kind:"{kind}"{unit_tag}}}, {{add}}))')
            print(f"  point created: {dis}")
            return find_ref(session, f'point and dis=="{dis}"')
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def ensure_site(session, dis: str) -> str:
    with _step(f"ensure_site:{dis}") as span:
        try:
            existing = find_ref(session, f'site and dis=="{dis}"')
            if existing:
                print(f"  site already exists: {dis}")
                return existing
            eval_axon(session, f'commit(diff(null, {{site, dis:"{dis}"}}, {{add}}))')
            print(f"  site created: {dis}")
            return find_ref(session, f'site and dis=="{dis}"')
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def ensure_equip(session, dis: str, site_ref: str, markers: list[str]) -> str:
    with _step(f"ensure_equip:{dis}") as span:
        try:
            existing = find_ref(session, f'equip and dis=="{dis}"')
            if existing:
                print(f"  equip already exists: {dis}")
                return existing
            marker_str = "".join(f", {m}" for m in markers)
            eval_axon(session, f'commit(diff(null, {{equip, dis:"{dis}", siteRef:@{site_ref}{marker_str}}}, {{add}}))')
            print(f"  equip created: {dis}")
            return find_ref(session, f'equip and dis=="{dis}"')
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def ensure_point_tags(session, point_ref: str, markers: list[str], equip_ref: str) -> None:
    """Tags an existing point in place (no {add} - the point already
    exists from Phase 1's ingestion wiring). Checks each marker/equipRef
    before committing so reruns are safe."""
    with _step(f"ensure_point_tags:{point_ref}") as span:
        try:
            rec = eval_axon(session, f"readById(@{point_ref})")[0]
            missing = [m for m in markers if rec.get(m) is None]
            needs_equip_ref = rec.get("equipRef") is None
            if not missing and not needs_equip_ref:
                print(f"  point {point_ref} already tagged")
                return
            parts = list(missing)
            if needs_equip_ref:
                parts.append(f"equipRef: @{equip_ref}")
            eval_axon(session, f'commit(diff(readById(@{point_ref}), {{{", ".join(parts)}}}, {{}}))')
            print(f"  tagged point {point_ref}: {parts}")
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def ensure_task(session, dis: str, conn_ref: str, topic: str, point_ref: str, value_expr: str) -> str:
    with _step(f"ensure_task:{dis}") as span:
        try:
            existing = find_ref(session, f'task and dis=="{dis}"')
            if existing:
                print(f"  task already exists: {dis}")
                return existing

            # value_expr may itself contain double quotes (e.g. == "active")
            # which must be escaped before embedding in the already-double-
            # quoted taskExpr string - this is exactly the kind of nested-
            # quoting bug that cost real time earlier tonight when hand-
            # typing axon in the shell.
            escaped_value_expr = value_expr.replace('"', '\\"')
            task_expr = (
                f'(msg) => do json: ioReadJson(msg->payload); '
                f'commit(diff(readById(@{point_ref}), {{curVal: {escaped_value_expr}, curStatus: \\"ok\\"}}, {{transient}})) end'
            )
            eval_axon(
                session,
                f'commit(diff(null, {{task, dis:"{dis}", obsMqtt, obsMqttConnRef: @{conn_ref}, '
                f'obsMqttTopic:"{topic}", taskExpr:"{task_expr}"}}, {{add}}))',
            )
            print(f"  task created: {dis}")
            return find_ref(session, f'task and dis=="{dis}"')
        except Exception as exc:
            span.set_attribute("error", str(exc))
            raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="http://localhost:8180/")
    parser.add_argument("--username", default="su")
    parser.add_argument("--password", default="windrow-demo-pass")
    args = parser.parse_args()

    tracing.init_tracing("windrow-provision-haxall")
    session = connect(args.uri, args.username, args.password)

    print("ensuring lib chain...")
    ensure_libs(session)

    conn_ref = find_ref(session, "mqttConn")
    if not conn_ref:
        raise SystemExit("No mqttConn found - run the original haxall-mqtt-integration.md setup first.")

    print("ensuring points...")
    bacnet_fan_ref = find_ref(session, 'point and dis=="Fan Status"')
    modbus_temp_ref = ensure_point(session, "Modbus Zone Temp", "Number", unit="°F")
    modbus_fan_ref = ensure_point(session, "Modbus Fan Status", "Bool")

    print("ensuring tasks...")
    ensure_task(
        session, "Windrow Ingest BACnet Fan Status", conn_ref,
        "fbf/mock-ahu-1/binaryValue,1", bacnet_fan_ref,
        'json->value == "active"',
    )
    ensure_task(
        session, "Windrow Ingest Modbus Zone Temp", conn_ref,
        "fbf/mock-modbus-1/zone-temp", modbus_temp_ref,
        "json->value",
    )
    ensure_task(
        session, "Windrow Ingest Modbus Fan Status", conn_ref,
        "fbf/mock-modbus-1/fan-status", modbus_fan_ref,
        "json->value == 1",
    )

    print("ensuring site/equip model...")
    site_ref = ensure_site(session, "Windrow Demo Site")
    ahu1_ref = ensure_equip(session, "AHU-1", site_ref, markers=["ahu"])
    ahu2_ref = ensure_equip(session, "AHU-2", site_ref, markers=["ahu"])

    print("tagging points...")
    bacnet_temp_ref = find_ref(session, 'point and dis=="Zone Temp"')
    ensure_point_tags(session, bacnet_temp_ref, ["zone", "air", "temp", "sensor"], ahu1_ref)
    ensure_point_tags(session, bacnet_fan_ref, ["fan", "run", "sensor"], ahu1_ref)
    ensure_point_tags(session, modbus_temp_ref, ["zone", "air", "temp", "sensor"], ahu2_ref)
    ensure_point_tags(session, modbus_fan_ref, ["fan", "run", "sensor"], ahu2_ref)

    print("ensuring write-demo fixture point...")
    ensure_point(session, "Windrow Write Demo", "Number", cur=False)

    print("done.")


if __name__ == "__main__":
    main()
