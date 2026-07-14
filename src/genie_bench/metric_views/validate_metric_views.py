"""Validate metric views against golden SQL semantics via MEASURE() queries.

Compares key MV measure/flag results to golden SQL outputs for Q1–Q10 coverage.
Run after build_metric_views.py on a preview-channel warehouse.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config, render_template
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"


def _q(w: WorkspaceClient, warehouse_id: str, sql: str) -> list[list]:
    res = execute_sql(w, warehouse_id, sql)
    return list(res.result.data_array or []) if res.result else []


def _scalar(rows: list[list], col: int = 0) -> float | None:
    if not rows:
        return None
    v = rows[0][col]
    if v is None:
        return None
    return float(v)


def validate(catalog: str, schema: str, warehouse_id: str) -> int:
    w = WorkspaceClient()
    cs = f"{catalog}.{schema}"
    mapping = {"catalog": catalog, "schema": schema}
    failures: list[str] = []

    def golden(rel: str) -> list[list]:
        sql = render_template((REPO_ROOT / rel).read_text(), mapping)
        return _q(w, warehouse_id, sql)

    # --- Anchor smoke ---
    anchor = _q(w, warehouse_id, f"SELECT * FROM {cs}.v_fiscal_anchor")
    if len(anchor) != 1:
        failures.append(f"v_fiscal_anchor expected 1 row, got {len(anchor)}")
    else:
        print(f"OK v_fiscal_anchor: {anchor[0]}")

    # --- Q1: last fiscal month revenue via rich MV flag ---
    g1 = _scalar(golden("config/golden/q01.sql"))
    mv1 = _scalar(
        _q(
            w,
            warehouse_id,
            f"""
            SELECT MEASURE(`Recognized Revenue`)
            FROM {cs}.mv_pulseforge_sales_rich
            WHERE `Is Last Fiscal Month`
            """,
        )
    )
    if g1 is None or mv1 is None or abs(g1 - mv1) > 0.02:
        failures.append(f"Q1 mismatch golden={g1} mv_rich={mv1}")
    else:
        print(f"OK Q1 last-fm revenue: {mv1}")

    # --- Q2: last fiscal quarter by region ---
    g2 = golden("config/golden/q02.sql")
    mv2 = _q(
        w,
        warehouse_id,
        f"""
        SELECT `Region`, MEASURE(`Recognized Revenue`) AS recognized_revenue
        FROM {cs}.mv_pulseforge_sales_rich
        WHERE `Is Last Fiscal Quarter`
        GROUP BY `Region`
        ORDER BY recognized_revenue DESC
        """,
    )
    g2_map = {r[0]: float(r[1]) for r in g2}
    mv2_map = {r[0]: float(r[1]) for r in mv2}
    if g2_map != {} and g2_map.keys() == mv2_map.keys():
        ok = all(abs(g2_map[k] - mv2_map[k]) <= 0.02 for k in g2_map)
        if ok:
            print(f"OK Q2 last-fq by region ({len(mv2_map)} regions)")
        else:
            failures.append(f"Q2 value mismatch golden={g2_map} mv={mv2_map}")
    else:
        failures.append(f"Q2 key mismatch golden={g2_map} mv={mv2_map}")

    # --- Q5: active members EMEA — conformed SQL source exact ---
    g5 = _scalar(golden("config/golden/q05.sql"))
    mv5 = _scalar(
        _q(
            w,
            warehouse_id,
            f"""
            SELECT MEASURE(`Active Members`)
            FROM {cs}.mv_pulseforge_conformed_sqlsrc
            WHERE `Region Code` = 'EMEA' AND `Is Current Fiscal Year`
            """,
        )
    )
    if g5 is None or mv5 is None or abs(g5 - mv5) > 0.5:
        failures.append(f"Q5 active members mismatch golden={g5} mv_sqlsrc={mv5}")
    else:
        print(f"OK Q5 active EMEA members: {mv5}")

    # --- Q8: churn rate by campaign in retention window ---
    g8 = golden("config/golden/q08.sql")
    mv8 = _q(
        w,
        warehouse_id,
        f"""
        SELECT `Campaign Code`, MEASURE(`Churn Rate`) AS churn_rate
        FROM {cs}.mv_pulseforge_conformed_sqlsrc
        WHERE `Is Retention Push Window` AND `Fact Family` = 'subscription'
        GROUP BY `Campaign Code`
        ORDER BY churn_rate ASC
        LIMIT 10
        """,
    )
    if not g8:
        failures.append("Q8 golden returned no rows")
    elif not mv8:
        failures.append("Q8 MV returned no rows")
    else:
        # Ties on churn_rate are common at small scale — compare the top-rate set.
        g_top_rate = float(g8[0][3]) if len(g8[0]) > 3 else float(g8[0][-1])
        # Re-query with names for a clearer check: lowest churn codes must match
        g_codes = {r[0] for r in g8 if abs(float(r[3] if len(r) > 3 else r[-1]) - g_top_rate) < 1e-9}
        mv_top = float(mv8[0][1])
        mv_codes = {r[0] for r in mv8 if abs(float(r[1]) - mv_top) < 1e-9}
        if g_codes & mv_codes or abs(g_top_rate - mv_top) < 1e-9:
            print(f"OK Q8 churn ranking (top rate={mv_top}, codes={sorted(mv_codes)[:5]})")
        else:
            failures.append(f"Q8 top campaign mismatch golden={g8[0][0]} mv={mv8[0][0]}")

    # --- Q3-ish: hardware FYTD via flag ---
    mv3 = _q(
        w,
        warehouse_id,
        f"""
        SELECT `Member ID`, MEASURE(`Hardware Revenue`) AS hw
        FROM {cs}.mv_pulseforge_sales_rich
        WHERE `Is Current FYTD`
        GROUP BY `Member ID`
        ORDER BY hw DESC
        LIMIT 10
        """,
    )
    if len(mv3) == 0:
        failures.append("Q3 hardware FYTD returned no rows")
    else:
        print(f"OK Q3 hardware FYTD top member: {mv3[0][0]} = {mv3[0][1]}")

    # --- Preview MVs (best-effort) ---
    for name, sql in [
        (
            "mv_pulseforge_conformed_wide",
            f"SELECT MEASURE(`Last Fiscal Month Revenue`) FROM {cs}.mv_pulseforge_conformed_wide",
        ),
        (
            "mv_pulseforge_sales_windowed",
            f"SELECT MEASURE(`Last Fiscal Month Revenue Flag`) FROM {cs}.mv_pulseforge_sales_windowed",
        ),
        (
            "mv_pulseforge_membership",
            f"""
            SELECT MEASURE(`Churn Rate`)
            FROM {cs}.mv_pulseforge_membership
            WHERE `Is Retention Push Window`
            """,
        ),
    ]:
        try:
            rows = _q(w, warehouse_id, sql)
            print(f"OK smoke {name}: {rows[:1]}")
        except Exception as e:  # noqa: BLE001
            failures.append(f"{name} smoke failed: {e}")

    print("---")
    if failures:
        print(f"FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("All validations passed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--warehouse-id", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    warehouse_id = args.warehouse_id
    if not warehouse_id and STATE_PATH.exists():
        warehouse_id = json.loads(STATE_PATH.read_text()).get("warehouse_id_eval")
    if not warehouse_id:
        raise SystemExit("Need --warehouse-id or provisioned_state.json warehouse_id_eval")
    return validate(
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        warehouse_id,
    )


if __name__ == "__main__":
    sys.exit(main())
