"""Create Unity Catalog SQL table functions used as Genie trusted assets (T17/T4/T21).

Genie commonly emits `SELECT * FROM catalog.schema.fn_name()`, so these MUST be
table-valued functions (RETURNS TABLE), not scalars.

T17 targets ~100% via trusted assets alone — one function (or pair) per question.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"


FUNCTIONS = [
    {
        "name": "fn_recognized_revenue_last_fiscal_month",
        "comment": "Total net recognized revenue for the previous fiscal month (4-4-5 calendar).",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_recognized_revenue_last_fiscal_month()
RETURNS TABLE (recognized_revenue DOUBLE)
LANGUAGE SQL
COMMENT 'PulseForge official recognized revenue for last fiscal month — call as SELECT * FROM fn_recognized_revenue_last_fiscal_month()'
RETURN
  WITH last_fm AS (
    SELECT fiscal_year, fiscal_month
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = add_months(current_date(), -1)
    LIMIT 1
  )
  SELECT ROUND(COALESCE(SUM(ol.net_recognized_amount), 0), 2) AS recognized_revenue
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
  JOIN last_fm lf
    ON d.fiscal_year = lf.fiscal_year
   AND d.fiscal_month = lf.fiscal_month
  WHERE ol.line_status = 'FULFILLED'
""",
    },
    {
        "name": "fn_recognized_revenue_by_region_last_fq",
        "comment": "Recognized revenue by geo region for last fiscal quarter.",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_recognized_revenue_by_region_last_fq()
RETURNS TABLE (region_name STRING, recognized_revenue DOUBLE)
LANGUAGE SQL
COMMENT 'PulseForge recognized revenue by region for last fiscal quarter'
RETURN
  WITH last_fq AS (
    SELECT fiscal_year, fiscal_quarter
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = add_months(current_date(), -3)
    LIMIT 1
  )
  SELECT
    g.region_name,
    ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
  JOIN {catalog}.{schema}.dim_geo g ON ol.geo_key = g.geo_key
  JOIN last_fq fq
    ON d.fiscal_year = fq.fiscal_year
   AND d.fiscal_quarter = fq.fiscal_quarter
  WHERE ol.line_status = 'FULFILLED'
  GROUP BY g.region_name
  ORDER BY recognized_revenue DESC
""",
    },
    {
        "name": "fn_top_members_hardware_fytd",
        "comment": "Top 10 members by lifetime recognized hardware revenue fiscal YTD.",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_top_members_hardware_fytd()
RETURNS TABLE (member_id STRING, display_handle STRING, hardware_revenue_fytd DOUBLE)
LANGUAGE SQL
COMMENT 'Top 10 members by hardware recognized revenue fiscal YTD'
RETURN
  WITH fytd AS (
    SELECT fiscal_year
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = current_date()
    LIMIT 1
  )
  SELECT
    m.member_id,
    m.display_handle,
    ROUND(SUM(ol.net_recognized_amount), 2) AS hardware_revenue_fytd
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_product p ON ol.product_key = p.product_key
  JOIN {catalog}.{schema}.dim_member m ON ol.member_key = m.member_key
  JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
  JOIN fytd ON d.fiscal_year = fytd.fiscal_year
  WHERE ol.line_status = 'FULFILLED'
    AND p.product_class IN ('DEVICE', 'ACCESSORY')
  GROUP BY m.member_id, m.display_handle
  ORDER BY hardware_revenue_fytd DESC
  LIMIT 10
""",
    },
    {
        "name": "fn_business_health_kpi_pack",
        "comment": "Business health KPI pack: FYTD recognized revenue, ending MRR, churn rate.",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_business_health_kpi_pack()
RETURNS TABLE (recognized_revenue_fytd DOUBLE, ending_mrr DOUBLE, churn_rate_fytd DOUBLE)
LANGUAGE SQL
COMMENT 'PulseForge business health KPI pack for ambiguous how-is-the-business-doing questions'
RETURN
  WITH fytd AS (
    SELECT fiscal_year
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = current_date()
    LIMIT 1
  ),
  rev AS (
    SELECT ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue_fytd
    FROM {catalog}.{schema}.fact_order_line ol
    JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
    JOIN fytd ON d.fiscal_year = fytd.fiscal_year
    WHERE ol.line_status = 'FULFILLED'
  ),
  mrr AS (
    SELECT ROUND(SUM(se.mrr_delta), 2) AS ending_mrr
    FROM {catalog}.{schema}.fact_subscription_event se
    JOIN {catalog}.{schema}.dim_date d ON se.event_date_key = d.date_key
    JOIN fytd ON d.fiscal_year = fytd.fiscal_year
  ),
  churn AS (
    SELECT
      COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END)
        / NULLIF(COUNT(DISTINCT se.member_key), 0) AS churn_rate_fytd
    FROM {catalog}.{schema}.fact_subscription_event se
    JOIN {catalog}.{schema}.dim_date d ON se.event_date_key = d.date_key
    JOIN fytd ON d.fiscal_year = fytd.fiscal_year
  )
  SELECT rev.recognized_revenue_fytd, mrr.ending_mrr, ROUND(churn.churn_rate_fytd, 4) AS churn_rate_fytd
  FROM rev CROSS JOIN mrr CROSS JOIN churn
""",
    },
    {
        "name": "fn_active_members_emea_fy",
        "comment": "Active EMEA members this fiscal year (activate/renew last 90d, no later cancel).",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_active_members_emea_fy()
RETURNS TABLE (active_members BIGINT)
LANGUAGE SQL
COMMENT 'PulseForge active EMEA member count this fiscal year — call as SELECT * FROM fn_active_members_emea_fy()'
RETURN
  WITH fy AS (
    SELECT fiscal_year
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = current_date()
    LIMIT 1
  ),
  last_events AS (
    SELECT
      se.member_key,
      MAX(CASE WHEN se.event_type IN ('ACTIVATE', 'RENEW') THEN d.calendar_date END) AS last_active_dt,
      MAX(CASE WHEN se.event_type = 'CANCEL' THEN d.calendar_date END) AS last_cancel_dt
    FROM {catalog}.{schema}.fact_subscription_event se
    JOIN {catalog}.{schema}.dim_date d ON se.event_date_key = d.date_key
    JOIN fy ON d.fiscal_year = fy.fiscal_year
    GROUP BY se.member_key
  )
  SELECT COUNT(*) AS active_members
  FROM last_events le
  JOIN {catalog}.{schema}.dim_member m ON le.member_key = m.member_key
  JOIN {catalog}.{schema}.dim_geo g ON m.home_geo_key = g.geo_key
  WHERE g.region_code = 'EMEA'
    AND le.last_active_dt >= date_sub(current_date(), 90)
    AND (le.last_cancel_dt IS NULL OR le.last_cancel_dt < le.last_active_dt)
""",
    },
    {
        "name": "fn_q1_vs_q2_revenue_by_family",
        "comment": "Fiscal Q1 vs Q2 recognized revenue by product family (current FY).",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_q1_vs_q2_revenue_by_family()
RETURNS TABLE (product_family STRING, q1_revenue DOUBLE, q2_revenue DOUBLE)
LANGUAGE SQL
COMMENT 'Fiscal Q1 vs Q2 recognized revenue by product family for the current fiscal year'
RETURN
  WITH fy AS (
    SELECT fiscal_year
    FROM {catalog}.{schema}.dim_date
    WHERE calendar_date = current_date()
    LIMIT 1
  )
  SELECT
    p.product_family,
    ROUND(SUM(CASE WHEN d.fiscal_quarter = 1 THEN ol.net_recognized_amount ELSE 0 END), 2) AS q1_revenue,
    ROUND(SUM(CASE WHEN d.fiscal_quarter = 2 THEN ol.net_recognized_amount ELSE 0 END), 2) AS q2_revenue
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_product p ON ol.product_key = p.product_key
  JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
  JOIN fy ON d.fiscal_year = fy.fiscal_year
  WHERE ol.line_status = 'FULFILLED'
    AND d.fiscal_quarter IN (1, 2)
  GROUP BY p.product_family
  ORDER BY p.product_family
""",
    },
    {
        "name": "fn_june_revenue_spike_attribution",
        "comment": "June 2025 fiscal revenue attribution by product family and campaign.",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_june_revenue_spike_attribution()
RETURNS TABLE (
  product_family STRING,
  sku_code STRING,
  campaign_code STRING,
  campaign_name STRING,
  fiscal_year INT,
  fiscal_month INT,
  recognized_revenue DOUBLE,
  order_count BIGINT
)
LANGUAGE SQL
COMMENT 'Why did revenue spike in June — PulseForge X1 launch attribution (fiscal June 2025)'
RETURN
  SELECT
    p.product_family,
    p.sku_code,
    c.campaign_code,
    c.campaign_name,
    d.fiscal_year,
    d.fiscal_month,
    ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue,
    COUNT(DISTINCT ol.order_key) AS order_count
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_product p ON ol.product_key = p.product_key
  JOIN {catalog}.{schema}.dim_campaign c ON ol.campaign_key = c.campaign_key
  JOIN {catalog}.{schema}.dim_date d ON ol.recognition_date_key = d.date_key
  WHERE d.fiscal_month = 6
    AND d.fiscal_year = 2025
    AND ol.line_status = 'FULFILLED'
  GROUP BY ALL
  ORDER BY recognized_revenue DESC
  LIMIT 20
""",
    },
    {
        "name": "fn_campaigns_churn_reduction",
        "comment": "Campaigns ranked by subscription churn rate in the retention push window.",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_campaigns_churn_reduction()
RETURNS TABLE (
  campaign_code STRING,
  campaign_name STRING,
  cancels BIGINT,
  touched_members BIGINT,
  churn_rate DOUBLE
)
LANGUAGE SQL
COMMENT 'Campaigns that drove subscription churn reduction (RETAIN-PLUS window 2025-04-01..2025-06-30)'
RETURN
  WITH cohort AS (
    SELECT
      c.campaign_code,
      c.campaign_name,
      COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END) AS cancels,
      COUNT(DISTINCT se.member_key) AS touched_members,
      COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END)
        / NULLIF(COUNT(DISTINCT se.member_key), 0) AS churn_rate
    FROM {catalog}.{schema}.fact_subscription_event se
    JOIN {catalog}.{schema}.dim_campaign c ON se.attribution_campaign_key = c.campaign_key
    JOIN {catalog}.{schema}.dim_date d ON se.event_date_key = d.date_key
    WHERE d.calendar_date BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
    GROUP BY c.campaign_code, c.campaign_name
  )
  SELECT *
  FROM cohort
  ORDER BY churn_rate ASC
  LIMIT 10
""",
    },
    {
        "name": "fn_recognized_revenue_for_sku",
        "comment": "Recognized revenue for a specific SKU code (empty/zero for nonexistent).",
        "ddl": """
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_recognized_revenue_for_sku(sku STRING)
RETURNS TABLE (sku_code STRING, recognized_revenue DOUBLE)
LANGUAGE SQL
COMMENT 'Recognized revenue for a product SKU — returns zero when SKU does not exist'
RETURN
  SELECT
    sku AS sku_code,
    ROUND(COALESCE(SUM(ol.net_recognized_amount), 0), 2) AS recognized_revenue
  FROM {catalog}.{schema}.fact_order_line ol
  JOIN {catalog}.{schema}.dim_product p ON ol.product_key = p.product_key
  WHERE p.sku_code = sku
    AND ol.line_status = 'FULFILLED'
""",
    },
]


def build_functions(
    catalog: str,
    schema: str,
    warehouse_id: str | None = None,
    grant_tiers: list[str] | None = None,
) -> list[str]:
    w = WorkspaceClient()
    if not warehouse_id:
        if STATE_PATH.exists():
            state = json.loads(STATE_PATH.read_text())
            warehouse_id = state.get("warehouse_id_eval")
        if not warehouse_id:
            whs = list(w.warehouses.list())
            if not whs:
                raise RuntimeError("No SQL warehouse available to create functions")
            warehouse_id = whs[0].id

    created = []
    for fn in FUNCTIONS:
        ddl = fn["ddl"].format(catalog=catalog, schema=schema)
        execute_sql(w, warehouse_id, ddl)
        fq = f"{catalog}.{schema}.{fn['name']}"
        # Smoke-test Genie's preferred invocation pattern (skip parameterized)
        if "(" not in fn["name"] and "for_sku" not in fn["name"]:
            smoke = execute_sql(w, warehouse_id, f"SELECT * FROM {fq}()")
            n = len(smoke.result.data_array or []) if smoke.result else 0
            print(f"Created TVF {fq} (smoke rows={n})", flush=True)
        else:
            smoke = execute_sql(w, warehouse_id, f"SELECT * FROM {fq}('ZX-NOEXIST-999')")
            n = len(smoke.result.data_array or []) if smoke.result else 0
            print(f"Created TVF {fq} (param smoke rows={n})", flush=True)
        created.append(fq)

    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
        wanted = {t.lower() for t in grant_tiers} if grant_tiers else None
        sps: list[str] = []
        for key, info in (state.get("tiers") or {}).items():
            if wanted is not None and key.lower() not in wanted:
                continue
            sp = info.get("sp_application_id")
            if sp:
                sps.append(sp)
        for fq in created:
            for sp in sps:
                sql = f"GRANT EXECUTE ON FUNCTION {fq} TO `{sp}`"
                try:
                    execute_sql(w, warehouse_id, sql)
                    print(f"OK: {sql}", flush=True)
                except Exception as e:  # noqa: BLE001
                    print(f"Grant warning ({sql}): {e}", flush=True)
    return created


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument(
        "--grant-tiers",
        nargs="*",
        default=None,
        help="Only GRANT EXECUTE to these tier keys (default: all provisioned)",
    )
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    build_functions(
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        args.warehouse_id,
        grant_tiers=args.grant_tiers,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
