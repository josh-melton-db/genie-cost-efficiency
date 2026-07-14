"""Materialize gold Delta tables from raw Volume parquet (prove-out path).

Full SDP pipeline remains in src/genie_bench/pipeline/ for bundle runs;
this script is the fast path for the T0 vs T4 validation loop.
"""

from __future__ import annotations

import sys

from genie_bench.config_utils import load_benchmark_config, volume_path


def _spark():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless(True).getOrCreate()


def materialize() -> None:
    from pyspark.sql import functions as F

    cfg = load_benchmark_config()
    catalog, schema = cfg["catalog"], cfg["schema"]
    root = volume_path(catalog, schema, "raw")
    spark = _spark()
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    dims = ["dim_date", "dim_geo", "dim_product", "dim_campaign", "dim_member"]
    facts = [
        "fact_order",
        "fact_order_line",
        "fact_subscription_event",
        "fact_usage_event",
        "fact_return",
    ]

    for name in dims + facts:
        path = f"{root}/{name}"
        print(f"Materializing {catalog}.{schema}.{name} from {path}", flush=True)
        df = spark.read.parquet(path)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{name}")
        )

    # Liquid clustering on primary facts (best-effort)
    for stmt in [
        f"ALTER TABLE {catalog}.{schema}.fact_order_line CLUSTER BY (recognition_date_key, product_key, member_key)",
        f"ALTER TABLE {catalog}.{schema}.fact_order CLUSTER BY (order_date_key, member_key)",
        f"ALTER TABLE {catalog}.{schema}.fact_subscription_event CLUSTER BY (event_date_key, member_key)",
        f"ALTER TABLE {catalog}.{schema}.fact_usage_event CLUSTER BY (event_date_key, member_key)",
    ]:
        try:
            spark.sql(stmt)
            print(f"OK {stmt}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"cluster note: {e}", flush=True)

    # T5 distractors
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_orders_v1 AS
        SELECT order_key AS id, order_gross_amount AS amount, order_date_key AS dt, member_key AS cust
        FROM {catalog}.{schema}.fact_order
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_orders_v2 AS
        SELECT order_id, order_gross_amount AS revenue, order_status, ship_date_key
        FROM {catalog}.{schema}.fact_order
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_customers_legacy AS
        SELECT member_key AS customer_key, display_handle AS name, member_tier
        FROM {catalog}.{schema}.dim_member
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_sales_flat AS
        SELECT order_line_key, gross_amount, order_date_key, product_key, member_key
        FROM {catalog}.{schema}.fact_order_line
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_mrr_daily AS
        SELECT event_date_key, SUM(mrr_delta) AS mrr
        FROM {catalog}.{schema}.fact_subscription_event
        GROUP BY event_date_key
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_workouts_raw AS
        SELECT * FROM {catalog}.{schema}.fact_usage_event
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_returns_old AS
        SELECT return_key AS rma_id, return_amount AS amt, product_key
        FROM {catalog}.{schema}.fact_return
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_campaign_spend AS
        SELECT campaign_key, 1000.0 AS spend_usd, campaign_code
        FROM {catalog}.{schema}.dim_campaign
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_geo_iso AS
        SELECT geo_key, country_code, region_code FROM {catalog}.{schema}.dim_geo
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_product_catalog_ext AS
        SELECT product_key, sku_code, list_price, product_family
        FROM {catalog}.{schema}.dim_product
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_member_pii_shadow AS
        SELECT member_key, display_handle, signup_date FROM {catalog}.{schema}.dim_member
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_fx_rates AS
        SELECT date_key, 'USD' AS ccy, 1.0 AS rate FROM {catalog}.{schema}.dim_date LIMIT 1000
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_fiscal_alt AS
        SELECT date_key, calendar_date,
               month(calendar_date) AS fiscal_month,
               quarter(calendar_date) AS fiscal_quarter,
               year(calendar_date) AS fiscal_year
        FROM {catalog}.{schema}.dim_date
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_nps_scores AS
        SELECT member_key, (member_key % 10) AS nps FROM {catalog}.{schema}.dim_member
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.{schema}.distractor_support_tickets AS
        SELECT member_key AS ticket_member, (member_key % 100) AS ticket_count
        FROM {catalog}.{schema}.dim_member
        """
    )
    print("Gold + distractors materialization complete.", flush=True)


def main() -> int:
    materialize()
    return 0


if __name__ == "__main__":
    sys.exit(main())
