"""Build dim_tier, metric_tco, and related rollup views for the dashboard."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import CONFIG_DIR, REPO_ROOT, load_benchmark_config
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"
TIERS_DIR = CONFIG_DIR / "tiers"


def seed_dim_tier(catalog: str, schema: str, warehouse_id: str) -> None:
    """Create/replace dim_tier from config/tiers/*.yaml (+ provisioned_state overrides)."""
    w = WorkspaceClient()
    rows = []
    state_tiers = {}
    if STATE_PATH.exists():
        state_tiers = (json.loads(STATE_PATH.read_text()).get("tiers") or {})

    for path in sorted(TIERS_DIR.glob("t*.yaml")):
        tier = yaml.safe_load(path.read_text())
        key = path.stem.lower()
        st = state_tiers.get(key) or {}
        rows.append(
            {
                "tier": tier.get("tier") or key.upper(),
                "tier_key": key,
                "name": tier.get("name") or "",
                "axis": tier.get("axis") or st.get("axis") or "unspecified",
                "lever": tier.get("lever") or st.get("lever") or tier.get("name") or "",
                "intent": (tier.get("intent") or st.get("intent") or "").replace("'", "''"),
            }
        )

    execute_sql(
        w,
        warehouse_id,
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.dim_tier (
          tier STRING,
          tier_key STRING,
          name STRING,
          axis STRING,
          lever STRING,
          intent STRING
        ) USING DELTA
        """,
    )
    if not rows:
        return
    values = ",\n".join(
        f"('{r['tier']}', '{r['tier_key']}', '{r['name']}', '{r['axis']}', '{r['lever']}', '{r['intent']}')"
        for r in rows
    )
    execute_sql(
        w,
        warehouse_id,
        f"INSERT INTO {catalog}.{schema}.dim_tier VALUES {values}",
    )
    print(f"Seeded {catalog}.{schema}.dim_tier with {len(rows)} tiers")


def refresh_metric_tco(catalog: str, schema: str, run_id: str, warehouse_id: str | None = None) -> None:
    w = WorkspaceClient()
    if not warehouse_id:
        state = json.loads(STATE_PATH.read_text())
        warehouse_id = state["warehouse_id_eval"]

    seed_dim_tier(catalog, schema, warehouse_id)

    select_sql = f"""
    WITH answers AS (
      SELECT
        run_id,
        tier,
        question_id,
        MAX(attempt) AS max_attempt,
        MAX(CASE WHEN attempt = 0 THEN correct END) AS first_pass_correct,
        MAX(correct) AS eventually_correct,
        MAX(latency_ms) AS latency_ms,
        MAX(regenerations_until_correct) AS regenerations_until_correct
      FROM {catalog}.{schema}.fact_benchmark_answer
      WHERE run_id = '{run_id}'
      GROUP BY run_id, tier, question_id
    ),
    quality AS (
      SELECT
        run_id,
        tier,
        COUNT(*) AS n_questions,
        AVG(CASE WHEN first_pass_correct THEN 1.0 ELSE 0.0 END) AS first_pass_accuracy,
        AVG(CASE WHEN eventually_correct THEN 1.0 ELSE 0.0 END) AS eventual_accuracy,
        AVG(latency_ms) AS avg_latency_ms,
        approx_percentile(latency_ms, 0.5) AS p50_latency_ms,
        approx_percentile(latency_ms, 0.95) AS p95_latency_ms,
        AVG(COALESCE(regenerations_until_correct, 0)) AS avg_regenerations,
        SUM(CASE WHEN eventually_correct THEN 1 ELSE 0 END) AS n_correct
      FROM answers
      GROUP BY run_id, tier
    ),
    costs AS (
      SELECT * FROM {catalog}.{schema}.fact_tier_cost WHERE run_id = '{run_id}'
    )
    SELECT
      q.run_id,
      q.tier,
      d.axis,
      d.lever,
      d.name AS tier_name,
      d.intent,
      q.n_questions,
      q.first_pass_accuracy,
      q.eventual_accuracy,
      q.avg_latency_ms,
      q.p50_latency_ms,
      q.p95_latency_ms,
      q.avg_regenerations,
      q.n_correct,
      COALESCE(c.genie_dbus, 0) AS genie_dbus,
      COALESCE(c.genie_cost_usd, 0) AS genie_cost_usd,
      COALESCE(c.warehouse_dbus, 0) AS warehouse_dbus,
      -- Warehouse columns are retained as diagnostics/backward-compatible schema.
      -- Primary benchmark cost is Genie LLM only.
      COALESCE(c.warehouse_cost_usd, 0) AS warehouse_cost_usd,
      COALESCE(c.warehouse_query_cost_usd, c.warehouse_cost_usd, 0) AS warehouse_query_cost_usd,
      COALESCE(c.warehouse_bill_cost_usd, 0) AS warehouse_bill_cost_usd,
      COALESCE(c.warehouse_hourly_usd, 0) AS warehouse_hourly_usd,
      COALESCE(c.genie_cost_usd, 0) AS total_cost_usd,
      COALESCE(c.bytes_scanned, 0) AS bytes_scanned,
      COALESCE(c.sql_statements, 0) AS sql_statements,
      COALESCE(c.total_duration_ms, 0) AS total_duration_ms,
      COALESCE(c.execution_duration_ms, 0) AS execution_duration_ms,
      COALESCE(c.waiting_for_compute_ms, 0) AS waiting_for_compute_ms,
      COALESCE(c.total_task_duration_ms, 0) AS total_task_duration_ms,
      COALESCE(c.execution_duration_sec, 0) AS execution_duration_sec,
      CASE WHEN q.n_correct > 0
        THEN COALESCE(c.genie_cost_usd, 0) / q.n_correct
        ELSE NULL END AS cost_per_correct_usd,
      CASE WHEN q.n_correct > 0
        THEN COALESCE(c.genie_cost_usd, 0) / q.n_correct
        ELSE NULL END AS genie_cost_per_correct_usd,
      CASE WHEN q.n_correct > 0
        THEN COALESCE(c.warehouse_cost_usd, 0) / q.n_correct
        ELSE NULL END AS warehouse_cost_per_correct_usd,
      CASE WHEN q.n_correct > 0
        THEN COALESCE(c.bytes_scanned, 0) / q.n_correct
        ELSE NULL END AS bytes_per_correct,
      CASE WHEN q.n_correct > 0
        THEN COALESCE(c.execution_duration_ms, 0) / q.n_correct
        ELSE NULL END AS execution_ms_per_correct,
      COALESCE(c.genie_cost_usd, 0) * (1.0 - q.eventual_accuracy) AS waste_index,
      CASE WHEN q.eventual_accuracy >= 0.50 THEN TRUE ELSE FALSE END AS above_quality_floor,
      current_timestamp() AS computed_at
    FROM quality q
    LEFT JOIN costs c ON q.run_id = c.run_id AND q.tier = c.tier
    LEFT JOIN {catalog}.{schema}.dim_tier d ON q.tier = d.tier
    """
    # Preserve other runs — upsert this run_id only.
    execute_sql(
        w,
        warehouse_id,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.metric_tco
        USING DELTA AS
        {select_sql}
        """,
    )
    # If the table already existed, CREATE IF NOT EXISTS is a no-op — replace this run's rows.
    execute_sql(
        w,
        warehouse_id,
        f"DELETE FROM {catalog}.{schema}.metric_tco WHERE run_id = '{run_id}'",
    )
    execute_sql(
        w,
        warehouse_id,
        f"INSERT INTO {catalog}.{schema}.metric_tco {select_sql}",
    )
    print(f"Refreshed {catalog}.{schema}.metric_tco for run {run_id}")

    # Axis rollup view for dashboard
    execute_sql(
        w,
        warehouse_id,
        f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.metric_tco_by_axis AS
        SELECT
          run_id,
          axis,
          COUNT(*) AS n_tiers,
          AVG(first_pass_accuracy) AS avg_first_pass_accuracy,
          AVG(eventual_accuracy) AS avg_eventual_accuracy,
          SUM(total_cost_usd) AS total_cost_usd,
          SUM(genie_cost_usd) AS genie_cost_usd,
          AVG(cost_per_correct_usd) AS avg_cost_per_correct_usd,
          AVG(genie_cost_per_correct_usd) AS avg_genie_cost_per_correct_usd,
          SUM(bytes_scanned) AS bytes_scanned,
          SUM(execution_duration_sec) AS execution_duration_sec,
          SUM(CASE WHEN above_quality_floor THEN 1 ELSE 0 END) AS n_above_floor
        FROM {catalog}.{schema}.metric_tco
        WHERE run_id = '{run_id}'
        GROUP BY run_id, axis
        """,
    )
    print(f"Refreshed {catalog}.{schema}.metric_tco_by_axis")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--seed-dim-only", action="store_true")
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    catalog = args.catalog or cfg["catalog"]
    schema = args.schema or cfg["schema"]
    state = json.loads(STATE_PATH.read_text())
    warehouse_id = state["warehouse_id_eval"]

    if args.seed_dim_only:
        seed_dim_tier(catalog, schema, warehouse_id)
        return 0

    run_id = args.run_id.strip()
    if not run_id:
        from genie_bench.run_ids import load_run_id, load_run_id_from_uc

        try:
            run_id = load_run_id()
        except FileNotFoundError:
            run_id = load_run_id_from_uc(catalog, schema, warehouse_id)
    refresh_metric_tco(catalog, schema, run_id, warehouse_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
