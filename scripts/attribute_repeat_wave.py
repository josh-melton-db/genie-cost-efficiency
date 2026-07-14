#!/usr/bin/env python3
"""Attribute Genie LLM cost across a multi-run wave with hour-bucket pro-rating.

Genie billing is hour-bucketed per service principal. When multiple benchmark
runs share an hour, naive per-run attribution double-counts that hour. This
script allocates each hour's Genie DBUs/cost to run_ids in proportion to the
number of ask attempts that SP made in that hour for each run.

Writes fact_tier_cost + refreshes metric_tco for every run_id, and dumps a
campaign summary JSON with mean/std $/correct across runs.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config
from genie_bench.cost.attribute_costs import (
    SQL_DIR,
    STATE_PATH,
    _exec_df,
    _hour_ceil_iso,
    _hour_floor_iso,
    _q,
    _render,
)
from genie_bench.report.build_results_tables import refresh_metric_tco
from genie_bench.sql_exec import execute_sql


def _load_run_ids(path: Path | None, run_ids: list[str] | None) -> list[str]:
    if run_ids:
        return [r.strip() for r in run_ids if r.strip()]
    if path and path.exists():
        return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]
    raise SystemExit("Pass --run-ids or --run-ids-file")


def attribute_wave(run_ids: list[str], catalog: str, schema: str, out_path: Path) -> dict:
    w = WorkspaceClient()
    state = json.loads(STATE_PATH.read_text())
    eval_wh = state["warehouse_id_eval"]
    run_list_sql = ", ".join(f"'{r}'" for r in run_ids)

    bounds = _exec_df(
        w,
        eval_wh,
        f"""
        SELECT
          date_format(MIN(started_at) - INTERVAL 5 MINUTES, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS start_ts,
          date_format(MAX(finished_at) + INTERVAL 5 MINUTES, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS end_ts
        FROM {catalog}.{schema}.fact_benchmark_answer
        WHERE run_id IN ({run_list_sql})
        """,
    )
    if bounds.empty or not bounds.iloc[0].get("start_ts"):
        raise RuntimeError("No answer rows for the given run_ids")
    start_ts = str(bounds.iloc[0]["start_ts"]).replace("+00:00", "Z")
    end_ts = str(bounds.iloc[0]["end_ts"]).replace("+00:00", "Z")
    bill_start = _hour_floor_iso(start_ts)
    bill_end = _hour_ceil_iso(end_ts)

    identity = _exec_df(
        w,
        eval_wh,
        f"""
        SELECT DISTINCT run_id, tier, space_id, warehouse_id, run_as_sp
        FROM {catalog}.{schema}.fact_benchmark_answer
        WHERE run_id IN ({run_list_sql})
        """,
    )
    sps = sorted({str(x) for x in identity["run_as_sp"].dropna().tolist() if str(x)})

    asks = _exec_df(
        w,
        eval_wh,
        f"""
        SELECT
          run_id,
          tier,
          run_as_sp,
          date_trunc('HOUR', started_at) AS usage_hour,
          COUNT(*) AS n_asks
        FROM {catalog}.{schema}.fact_benchmark_answer
        WHERE run_id IN ({run_list_sql})
        GROUP BY ALL
        """,
    )
    genie = _exec_df(
        w,
        eval_wh,
        _render(
            "genie_billing_by_sp.sql",
            start_ts=bill_start,
            end_ts=bill_end,
            sp_list=_q(sps),
        ),
    )
    print(
        f"Wave bill_window=[{bill_start}, {bill_end}) runs={len(run_ids)} "
        f"genie_rows={len(genie)} ask_buckets={len(asks)}",
        flush=True,
    )
    if genie.empty:
        raise RuntimeError("No GENIE billing rows yet — settle window not ready")

    # Normalize hour keys
    genie = genie.copy()
    if "usage_date" in genie.columns and "usage_start_time" not in genie.columns:
        # genie_billing_by_sp groups by usage_date; re-query hour grain
        genie = _exec_df(
            w,
            eval_wh,
            f"""
            SELECT
              date_trunc('HOUR', u.usage_start_time) AS usage_hour,
              u.identity_metadata.run_as AS run_as_sp,
              SUM(u.usage_quantity) AS billable_dbus,
              SUM(u.usage_quantity * lp.pricing.effective_list.default) AS list_cost_usd
            FROM system.billing.usage u
            JOIN system.billing.list_prices lp
              ON u.cloud = lp.cloud
             AND u.sku_name = lp.sku_name
             AND u.usage_start_time >= lp.price_start_time
             AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
            WHERE u.billing_origin_product = 'GENIE'
              AND u.usage_start_time < TIMESTAMP '{bill_end}'
              AND COALESCE(u.usage_end_time, u.usage_start_time + INTERVAL 1 HOUR) > TIMESTAMP '{bill_start}'
              AND u.identity_metadata.run_as IN ({_q(sps)})
            GROUP BY ALL
            """,
        )

    asks["usage_hour"] = pd.to_datetime(asks["usage_hour"], utc=True)
    genie["usage_hour"] = pd.to_datetime(genie["usage_hour"], utc=True)
    asks["n_asks"] = pd.to_numeric(asks["n_asks"], errors="coerce").fillna(0)
    genie["billable_dbus"] = pd.to_numeric(genie["billable_dbus"], errors="coerce").fillna(0)
    genie["list_cost_usd"] = pd.to_numeric(genie["list_cost_usd"], errors="coerce").fillna(0)

    # Denominator: asks per (sp, hour) across all runs
    denom = asks.groupby(["run_as_sp", "usage_hour"], as_index=False)["n_asks"].sum()
    denom = denom.rename(columns={"n_asks": "n_asks_hour"})
    asks = asks.merge(denom, on=["run_as_sp", "usage_hour"], how="left")
    asks["share"] = asks["n_asks"] / asks["n_asks_hour"].clip(lower=1)

    alloc = asks.merge(genie, on=["run_as_sp", "usage_hour"], how="left")
    alloc["billable_dbus"] = alloc["billable_dbus"].fillna(0) * alloc["share"]
    alloc["list_cost_usd"] = alloc["list_cost_usd"].fillna(0) * alloc["share"]

    per_run_tier = (
        alloc.groupby(["run_id", "tier", "run_as_sp"], as_index=False)[["billable_dbus", "list_cost_usd"]]
        .sum()
    )
    meta = identity.drop_duplicates(["run_id", "tier"])
    per_run_tier = per_run_tier.merge(
        meta[["run_id", "tier", "space_id", "warehouse_id"]],
        on=["run_id", "tier"],
        how="left",
    )

    execute_sql(
        w,
        eval_wh,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_tier_cost (
          run_id STRING, tier STRING, space_id STRING, warehouse_id STRING, run_as_sp STRING,
          genie_dbus DOUBLE, genie_cost_usd DOUBLE,
          warehouse_dbus DOUBLE, warehouse_bill_cost_usd DOUBLE, warehouse_hourly_usd DOUBLE,
          warehouse_query_cost_usd DOUBLE, warehouse_cost_usd DOUBLE,
          bytes_scanned DOUBLE, sql_statements DOUBLE,
          total_duration_ms DOUBLE, execution_duration_ms DOUBLE, waiting_for_compute_ms DOUBLE,
          total_task_duration_ms DOUBLE, execution_duration_sec DOUBLE,
          audit_events DOUBLE, computed_at TIMESTAMP
        ) USING DELTA
        """,
    )
    for rid in run_ids:
        execute_sql(w, eval_wh, f"DELETE FROM {catalog}.{schema}.fact_tier_cost WHERE run_id = '{rid}'")

    for row in per_run_tier.to_dict("records"):
        insert = f"""
        INSERT INTO {catalog}.{schema}.fact_tier_cost VALUES (
          '{row["run_id"]}', '{row["tier"]}', '{row.get("space_id") or ""}',
          '{row.get("warehouse_id") or ""}', '{row["run_as_sp"]}',
          {float(row["billable_dbus"])}, {float(row["list_cost_usd"])},
          0,0,0,0,0, 0,0, 0,0,0,0,0, 0, current_timestamp()
        )
        """
        execute_sql(w, eval_wh, insert)
        print(
            f"  {row['run_id']} {row['tier']}: ${float(row['list_cost_usd']):.4f} "
            f"({float(row['billable_dbus']):.4f} DBU)",
            flush=True,
        )

    for rid in run_ids:
        refresh_metric_tco(catalog, schema, rid, eval_wh)

    metrics = _exec_df(
        w,
        eval_wh,
        f"""
        SELECT run_id, tier, axis, lever, tier_name,
               first_pass_accuracy, eventual_accuracy, n_correct, n_questions,
               genie_dbus, genie_cost_usd, total_cost_usd, cost_per_correct_usd
        FROM {catalog}.{schema}.metric_tco
        WHERE run_id IN ({run_list_sql})
        """,
    )
    for col in [
        "first_pass_accuracy",
        "eventual_accuracy",
        "n_correct",
        "n_questions",
        "genie_dbus",
        "genie_cost_usd",
        "total_cost_usd",
        "cost_per_correct_usd",
    ]:
        if col in metrics.columns:
            metrics[col] = pd.to_numeric(metrics[col], errors="coerce")

    summary_rows = []
    for tier, g in metrics.groupby("tier"):
        costs = g["cost_per_correct_usd"].dropna()
        accs = g["eventual_accuracy"].dropna()
        genie_costs = g["genie_cost_usd"].dropna()
        mean = float(costs.mean()) if len(costs) else None
        std = float(costs.std(ddof=1)) if len(costs) > 1 else 0.0
        summary_rows.append(
            {
                "tier": tier,
                "tier_name": str(g["tier_name"].iloc[0]) if "tier_name" in g else tier,
                "axis": str(g["axis"].iloc[0]) if "axis" in g else None,
                "lever": str(g["lever"].iloc[0]) if "lever" in g else None,
                "n_runs": int(len(g)),
                "mean_accuracy": float(accs.mean()) if len(accs) else None,
                "std_accuracy": float(accs.std(ddof=1)) if len(accs) > 1 else 0.0,
                "mean_genie_cost_usd": float(genie_costs.mean()) if len(genie_costs) else None,
                "mean_cost_per_correct_usd": mean,
                "std_cost_per_correct_usd": std,
                "se_cost_per_correct_usd": (std / math.sqrt(len(costs))) if len(costs) else None,
                "min_cost_per_correct_usd": float(costs.min()) if len(costs) else None,
                "max_cost_per_correct_usd": float(costs.max()) if len(costs) else None,
            }
        )
    summary_rows.sort(key=lambda r: (r["mean_cost_per_correct_usd"] is None, r["mean_cost_per_correct_usd"] or 9e9))

    out = {
        "run_ids": run_ids,
        "ask_window": {"start": start_ts, "end": end_ts},
        "bill_window": {"start": bill_start, "end": bill_end},
        "attribution": "hour_bucket_prorated_by_ask_count",
        "per_run": metrics.to_dict("records"),
        "leaderboard": summary_rows,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote {out_path}", flush=True)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-ids", nargs="*", default=None)
    p.add_argument("--run-ids-file", default="logs/repeat5/run_ids.txt")
    p.add_argument("--catalog", default=None)
    p.add_argument("--schema", default=None)
    p.add_argument("--out", default="logs/repeat5/wave_summary.json")
    args = p.parse_args(argv)
    cfg = load_benchmark_config()
    run_ids = _load_run_ids(Path(args.run_ids_file) if args.run_ids_file else None, args.run_ids)
    attribute_wave(
        run_ids,
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        Path(args.out),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
