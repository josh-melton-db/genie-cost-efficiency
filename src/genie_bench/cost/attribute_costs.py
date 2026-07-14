"""Attribute Genie LLM costs per tier after the billing settle window.

The benchmark is focused on context strategy, so the primary cost metric is
Genie list cost only. SQL warehouse telemetry columns are retained for schema
compatibility, but populated as zero by this attribution path.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config
from genie_bench.report.build_results_tables import refresh_metric_tco
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"
SQL_DIR = Path(__file__).parent / "cost_sql"

# Fallback if list_prices lookup fails (Enterprise serverless SQL, US East).
DEFAULT_USD_PER_DBU = 0.70


def _q(ids: list[str]) -> str:
    return ", ".join(f"'{i}'" for i in ids if i)


def _render(name: str, **params: str) -> str:
    text = (SQL_DIR / name).read_text()
    for k, v in params.items():
        text = text.replace("{{" + k + "}}", v)
    return text


def _exec_df(w: WorkspaceClient, warehouse_id: str, sql: str) -> pd.DataFrame:
    res = execute_sql(w, warehouse_id, sql)
    try:
        cols = [c.name for c in res.manifest.schema.columns]  # type: ignore
        data = res.result.data_array if res.result else []  # type: ignore
        return pd.DataFrame(data or [], columns=cols)
    except Exception:
        return pd.DataFrame()


def _sum_num(df: pd.DataFrame, mask, col: str) -> float:
    if df.empty or col not in df.columns or mask is None:
        return 0.0
    try:
        series = pd.to_numeric(df.loc[mask, col], errors="coerce").fillna(0.0)
    except Exception:
        return 0.0
    return float(series.sum())


def _hour_floor_iso(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z")


def _hour_ceil_iso(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    floored = dt.replace(minute=0, second=0, microsecond=0)
    if floored < dt:
        floored += timedelta(hours=1)
    return floored.isoformat().replace("+00:00", "Z")


def _resolve_hourly_rate(cfg: dict, rate_df: pd.DataFrame) -> tuple[float, float, float, str]:
    wh_cfg = cfg.get("warehouse", {}) or {}
    size = str(wh_cfg.get("size", "Medium"))
    dbus_map = wh_cfg.get("dbus_per_hour") or {}
    dbus_per_hour = float(dbus_map.get(size, 24))

    usd_per_dbu = DEFAULT_USD_PER_DBU
    sku = "fallback"
    if not rate_df.empty and "usd_per_dbu" in rate_df.columns:
        raw = rate_df.iloc[0].get("usd_per_dbu")
        if raw is not None and str(raw).strip() not in ("", "None"):
            usd_per_dbu = float(raw)
            sku = str(rate_df.iloc[0].get("sku_name") or "list_prices")
    hourly_usd = dbus_per_hour * usd_per_dbu
    return hourly_usd, dbus_per_hour, usd_per_dbu, sku


def attribute_costs(
    run_id: str,
    catalog: str,
    schema: str,
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> None:
    w = WorkspaceClient()
    state = json.loads(STATE_PATH.read_text())
    eval_wh = state["warehouse_id_eval"]

    if not end_ts:
        end_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if not start_ts:
        bounds = _exec_df(
            w,
            eval_wh,
            f"""
            SELECT
              date_format(MIN(started_at) - INTERVAL 5 MINUTES, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS start_ts,
              date_format(MAX(finished_at) + INTERVAL 5 MINUTES, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") AS end_ts
            FROM {catalog}.{schema}.fact_benchmark_answer
            WHERE run_id = '{run_id}'
            """,
        )
        if not bounds.empty and bounds.iloc[0].get("start_ts"):
            start_ts = str(bounds.iloc[0]["start_ts"]).replace("+00:00", "Z")
            end_ts = str(bounds.iloc[0]["end_ts"]).replace("+00:00", "Z")
        else:
            start_ts = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat().replace("+00:00", "Z")

    # Genie billing is hour-bucketed, so use hour overlap with the run window.
    try:
        bill_start_ts = _hour_floor_iso(start_ts)
        bill_end_ts = _hour_ceil_iso(end_ts)
    except Exception:  # noqa: BLE001
        bill_start_ts, bill_end_ts = start_ts, end_ts

    run_identity = _exec_df(
        w,
        eval_wh,
        f"""
        SELECT DISTINCT
          tier,
          space_id,
          warehouse_id,
          run_as_sp
        FROM {catalog}.{schema}.fact_benchmark_answer
        WHERE run_id = '{run_id}'
        """,
    )
    if run_identity.empty:
        raise RuntimeError(f"No benchmark rows found for run_id={run_id}")
    sps = sorted(set(str(x) for x in run_identity["run_as_sp"].dropna().tolist() if str(x)))
    run_tier_set = set(run_identity["tier"].astype(str).tolist())

    print(
        f"Attributing Genie costs run_id={run_id} ask_window=[{start_ts}, {end_ts}) "
        f"bill_window=[{bill_start_ts}, {bill_end_ts}) tiers={sorted(run_tier_set)}",
        flush=True,
    )

    genie = _exec_df(
        w,
        eval_wh,
        _render(
            "genie_billing_by_sp.sql",
            start_ts=bill_start_ts,
            end_ts=bill_end_ts,
            sp_list=_q(sps),
        ),
    )
    print(f"frames: genie={len(genie)}", flush=True)
    if genie.empty:
        print(
            "No GENIE billing rows found for this run window yet; billing likely has not settled. "
            "No cost rows were written.",
            flush=True,
        )
        return

    # Preserve other runs — only replace rows for this run_id.
    execute_sql(
        w,
        eval_wh,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_tier_cost (
          run_id STRING,
          tier STRING,
          space_id STRING,
          warehouse_id STRING,
          run_as_sp STRING,
          genie_dbus DOUBLE,
          genie_cost_usd DOUBLE,
          warehouse_dbus DOUBLE,
          warehouse_bill_cost_usd DOUBLE,
          warehouse_hourly_usd DOUBLE,
          warehouse_query_cost_usd DOUBLE,
          warehouse_cost_usd DOUBLE,
          bytes_scanned DOUBLE,
          sql_statements DOUBLE,
          total_duration_ms DOUBLE,
          execution_duration_ms DOUBLE,
          waiting_for_compute_ms DOUBLE,
          total_task_duration_ms DOUBLE,
          execution_duration_sec DOUBLE,
          audit_events DOUBLE,
          computed_at TIMESTAMP
        ) USING DELTA
        """,
    )
    execute_sql(
        w,
        eval_wh,
        f"DELETE FROM {catalog}.{schema}.fact_tier_cost WHERE run_id = '{run_id}'",
    )

    for row in run_identity.to_dict("records"):
        tier = str(row["tier"])
        sp = str(row["run_as_sp"])
        space = str(row.get("space_id") or "")
        warehouse = str(row.get("warehouse_id") or "")

        genie_mask = (
            (genie["run_as_sp"] == sp) if (not genie.empty and "run_as_sp" in genie.columns) else None
        )

        genie_dbus = _sum_num(genie, genie_mask, "billable_dbus")
        genie_cost = _sum_num(genie, genie_mask, "list_cost_usd")

        insert = f"""
        INSERT INTO {catalog}.{schema}.fact_tier_cost VALUES (
          '{run_id}', '{tier}', '{space}', '{warehouse}', '{sp}',
          {genie_dbus}, {genie_cost}, 0.0, 0.0,
          0.0, 0.0, 0.0,
          0.0, 0.0,
          0.0, 0.0, 0.0,
          0.0, 0.0,
          0.0, current_timestamp()
        )
        """
        execute_sql(w, eval_wh, insert)
        print(
            f"  {tier}: genie=${genie_cost:.4f} dbus={genie_dbus:.4f}",
            flush=True,
        )

    refresh_metric_tco(catalog, schema, run_id, eval_wh)
    print("Cost attribution complete.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--start-ts", default=None)
    parser.add_argument("--end-ts", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    run_id = args.run_id.strip()
    if not run_id:
        from genie_bench.run_ids import load_run_id, load_run_id_from_uc

        try:
            run_id = load_run_id()
        except FileNotFoundError:
            state = json.loads(STATE_PATH.read_text())
            run_id = load_run_id_from_uc(
                args.catalog or cfg["catalog"],
                args.schema or cfg["schema"],
                state["warehouse_id_eval"],
            )
    attribute_costs(
        run_id,
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        args.start_ts,
        args.end_ts,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
