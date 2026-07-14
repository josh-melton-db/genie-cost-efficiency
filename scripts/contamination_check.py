#!/usr/bin/env python3
"""
Contamination guardrail CLI.

Runs evaluate-derived accuracy checks (or re-reads metric_tco) and exits
non-zero if T0 looks memorized / too easy. Also reports quality-floor coverage.
"""

from __future__ import annotations

import argparse
import json
import sys

from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", default="")
    p.add_argument("--catalog", default=None)
    p.add_argument("--schema", default=None)
    args = p.parse_args(argv)
    cfg = load_benchmark_config()
    catalog = args.catalog or cfg["catalog"]
    schema = args.schema or cfg["schema"]
    cont = cfg["contamination"]

    run_id = args.run_id.strip()
    if not run_id:
        from genie_bench.run_ids import load_run_id, load_run_id_from_uc

        try:
            run_id = load_run_id()
        except FileNotFoundError:
            state = json.loads(STATE_PATH.read_text())
            run_id = load_run_id_from_uc(catalog, schema, state["warehouse_id_eval"])

    w = WorkspaceClient()
    state = json.loads(STATE_PATH.read_text())
    wh = state["warehouse_id_eval"]
    # Prefer enriched metric_tco; fall back if above_quality_floor not yet refreshed
    try:
        res = execute_sql(
            w,
            wh,
            (
                f"SELECT tier, first_pass_accuracy, eventual_accuracy, "
                f"COALESCE(above_quality_floor, false) AS above_quality_floor "
                f"FROM {catalog}.{schema}.metric_tco WHERE run_id = '{run_id}'"
            ),
        )
    except Exception:
        res = execute_sql(
            w,
            wh,
            (
                f"SELECT tier, first_pass_accuracy, eventual_accuracy, "
                f"CAST(eventual_accuracy >= 0.5 AS BOOLEAN) AS above_quality_floor "
                f"FROM {catalog}.{schema}.metric_tco WHERE run_id = '{run_id}'"
            ),
        )
    rows = res.result.data_array if res.result else []
    cols = [c.name for c in res.manifest.schema.columns]  # type: ignore
    by_tier = {
        r[cols.index("tier")]: float(r[cols.index("first_pass_accuracy")]) for r in rows or []
    }
    n_above = sum(
        1
        for r in rows or []
        if str(r[cols.index("above_quality_floor")]).lower() in {"true", "1"}
    )

    t0 = by_tier.get("T0")
    full_tier = cont.get("full_tier", "T4")
    full_acc = by_tier.get(full_tier)
    print(
        f"run_id={run_id} T0 first_pass_accuracy={t0} "
        f"{full_tier} first_pass_accuracy={full_acc} tiers_above_floor={n_above}"
    )
    if t0 is not None and t0 > float(cont["t0_max_first_pass_accuracy"]):
        print("FAIL: T0 too accurate — dataset likely contaminated or too easy")
        return 2
    min_full = float(
        cont.get("full_tier_min_first_pass_accuracy")
        or cont.get("t4_min_first_pass_accuracy", 0.70)
    )
    if full_acc is not None and full_acc < min_full:
        print(f"WARN: {full_tier} below expected accuracy — curation may be incomplete")
    min_above = int(cont.get("min_tiers_above_floor", 0))
    if n_above < min_above:
        print(
            f"WARN: only {n_above} tiers above quality floor "
            f"(want ≥{min_above}) — TCO ranking will be sparse"
        )
    print("PASS: contamination guardrail")
    return 0


if __name__ == "__main__":
    sys.exit(main())
