"""
Lakeflow Spark Declarative Pipeline — PulseForge bronze → silver → gold.

Serverless SDP. Auto Loader from UC Volume raw files.
Gold facts use date partitioning + liquid clustering so pruned SQL is cheap
and bad full-scan SQL is expensive (tier-differentiated warehouse cost).

Catalog/schema/volume resolved from pipeline configuration parameters.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F


def _p(name: str, default: str = "") -> str:
    try:
        return spark.conf.get(f"bundle.var.{name}", spark.conf.get(name, default))  # noqa: F821
    except Exception:
        return default


def catalog() -> str:
    return _p("catalog", "genie_tco")


def schema() -> str:
    return _p("schema", "bench")


def raw_root() -> str:
    return f"/Volumes/{catalog()}/{schema()}/raw"


def _read_raw(entity: str):
    path = f"{raw_root()}/{entity}"
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(path)
    )


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

BRONZE_ENTITIES = [
    "dim_date",
    "dim_geo",
    "dim_product",
    "dim_campaign",
    "dim_member",
    "fact_order",
    "fact_order_line",
    "fact_subscription_event",
    "fact_usage_event",
    "fact_return",
]


def _make_bronze(entity: str):
    @dlt.table(
        name=f"bronze_{entity}",
        comment=f"Bronze Auto Loader ingest for {entity}",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return _read_raw(entity).withColumn("_ingest_ts", F.current_timestamp())

    return _bronze


for _entity in BRONZE_ENTITIES:
    globals()[f"bronze_{_entity}"] = _make_bronze(_entity)


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------


@dlt.table(name="silver_dim_date", comment="Clean fiscal calendar (4-4-5)")
@dlt.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
def silver_dim_date():
    return (
        dlt.read_stream("bronze_dim_date")
        .dropDuplicates(["date_key"])
        .select(
            F.col("date_key").cast("int").alias("date_key"),
            F.col("calendar_date").cast("date").alias("calendar_date"),
            F.col("fiscal_year").cast("int").alias("fiscal_year"),
            F.col("fiscal_month").cast("int").alias("fiscal_month"),
            F.col("fiscal_quarter").cast("int").alias("fiscal_quarter"),
            F.col("iso_week").cast("int").alias("iso_week"),
            F.col("date_label").cast("string").alias("date_label"),
        )
    )


@dlt.table(name="silver_dim_geo", comment="Geo dimension")
@dlt.expect_or_drop("valid_geo_key", "geo_key IS NOT NULL")
def silver_dim_geo():
    return dlt.read_stream("bronze_dim_geo").dropDuplicates(["geo_key"])


@dlt.table(name="silver_dim_product", comment="Product dimension — devices, accessories, plans")
@dlt.expect_or_drop("valid_product_key", "product_key IS NOT NULL")
@dlt.expect_or_drop("known_class", "product_class IN ('DEVICE','ACCESSORY','SUBSCRIPTION')")
def silver_dim_product():
    return dlt.read_stream("bronze_dim_product").dropDuplicates(["product_key"])


@dlt.table(name="silver_dim_campaign", comment="Marketing campaigns")
@dlt.expect_or_drop("valid_campaign_key", "campaign_key IS NOT NULL")
def silver_dim_campaign():
    return dlt.read_stream("bronze_dim_campaign").dropDuplicates(["campaign_key"])


@dlt.table(name="silver_dim_member", comment="Member dimension")
@dlt.expect_or_drop("valid_member_key", "member_key IS NOT NULL")
def silver_dim_member():
    return dlt.read_stream("bronze_dim_member").dropDuplicates(["member_key"])


@dlt.table(name="silver_fact_order", comment="Orders — typed, deduped")
@dlt.expect_or_drop("valid_order_key", "order_key IS NOT NULL")
def silver_fact_order():
    return dlt.read_stream("bronze_fact_order").dropDuplicates(["order_key"])


@dlt.table(name="silver_fact_order_line", comment="Order lines with net recognized amount")
@dlt.expect_or_drop("valid_line_key", "order_line_key IS NOT NULL")
@dlt.expect_or_drop("non_null_recognized", "net_recognized_amount IS NOT NULL")
def silver_fact_order_line():
    return dlt.read_stream("bronze_fact_order_line").dropDuplicates(["order_line_key"])


@dlt.table(name="silver_fact_subscription_event", comment="Subscription lifecycle events")
@dlt.expect_or_drop("valid_event", "subscription_event_key IS NOT NULL")
@dlt.expect_or_drop("known_event_type", "event_type IN ('ACTIVATE','RENEW','CANCEL')")
def silver_fact_subscription_event():
    return dlt.read_stream("bronze_fact_subscription_event").dropDuplicates(["subscription_event_key"])


@dlt.table(name="silver_fact_usage_event", comment="Workout / usage telemetry (high volume)")
@dlt.expect_or_drop("valid_usage", "usage_event_key IS NOT NULL")
def silver_fact_usage_event():
    return dlt.read_stream("bronze_fact_usage_event").dropDuplicates(["usage_event_key"])


@dlt.table(name="silver_fact_return", comment="RMA / returns")
@dlt.expect_or_drop("valid_return", "return_key IS NOT NULL")
def silver_fact_return():
    return dlt.read_stream("bronze_fact_return").dropDuplicates(["return_key"])


# ---------------------------------------------------------------------------
# Gold — conformed dims/facts with clustering for SQL-cost differentiation
# ---------------------------------------------------------------------------

CLUSTER_PROPS = {
    "delta.enableDeletionVectors": "true",
}


@dlt.table(
    name="dim_date",
    comment="Gold fiscal date dimension (4-4-5). Use fiscal_* for period questions.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
)
def gold_dim_date():
    return dlt.read("silver_dim_date")


@dlt.table(
    name="dim_geo",
    comment="Gold geo dimension. region_code values: NA, EMEA, APAC, LATAM.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
)
def gold_dim_geo():
    return dlt.read("silver_dim_geo")


@dlt.table(
    name="dim_product",
    comment="Gold product dimension. product_class: DEVICE, ACCESSORY, SUBSCRIPTION.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
)
def gold_dim_product():
    return dlt.read("silver_dim_product")


@dlt.table(
    name="dim_campaign",
    comment="Gold campaign dimension. Includes PFX1-LAUNCH-2025 and RETAIN-PLUS-2025Q2.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
)
def gold_dim_campaign():
    return dlt.read("silver_dim_campaign")


@dlt.table(
    name="dim_member",
    comment="Gold member dimension. member_tier: Spark, Volt, Forge, EliteForge.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
)
def gold_dim_member():
    return dlt.read("silver_dim_member")


@dlt.table(
    name="fact_order",
    comment="Gold orders. order_gross_amount is NOT recognized revenue.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
    cluster_by=["order_date_key", "member_key"],
)
def gold_fact_order():
    return dlt.read("silver_fact_order")


@dlt.table(
    name="fact_order_line",
    comment=(
        "Gold order lines. Official revenue = net_recognized_amount where "
        "line_status='FULFILLED', dated by recognition_date_key."
    ),
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
    cluster_by=["recognition_date_key", "product_key", "member_key"],
)
def gold_fact_order_line():
    return dlt.read("silver_fact_order_line")


@dlt.table(
    name="fact_subscription_event",
    comment="Gold subscription events for MRR and churn.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
    cluster_by=["event_date_key", "member_key"],
)
def gold_fact_subscription_event():
    return dlt.read("silver_fact_subscription_event")


@dlt.table(
    name="fact_usage_event",
    comment="Gold workout telemetry — high volume; do NOT use for revenue KPIs.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
    cluster_by=["event_date_key", "member_key"],
)
def gold_fact_usage_event():
    return dlt.read("silver_fact_usage_event")


@dlt.table(
    name="fact_return",
    comment="Gold returns / RMAs.",
    table_properties={**CLUSTER_PROPS, "quality": "gold"},
    cluster_by=["return_date_key"],
)
def gold_fact_return():
    return dlt.read("silver_fact_return")


# ---------------------------------------------------------------------------
# Distractor views for T5 anti-pattern (materialized as gold tables)
# ---------------------------------------------------------------------------


@dlt.table(name="distractor_orders_v1", comment="LEGACY orders extract — prefer fact_order")
def distractor_orders_v1():
    return dlt.read("silver_fact_order").select(
        F.col("order_key").alias("id"),
        F.col("order_gross_amount").alias("amount"),
        F.col("order_date_key").alias("dt"),
        F.col("member_key").alias("cust"),
    )


@dlt.table(name="distractor_orders_v2", comment="Alternate orders — conflicting grain")
def distractor_orders_v2():
    return dlt.read("silver_fact_order").select(
        "order_id",
        F.col("order_gross_amount").alias("revenue"),
        "order_status",
        "ship_date_key",
    )


@dlt.table(name="distractor_customers_legacy", comment="Legacy customer dump")
def distractor_customers_legacy():
    return dlt.read("silver_dim_member").select(
        F.col("member_key").alias("customer_key"),
        F.col("display_handle").alias("name"),
        "member_tier",
    )


@dlt.table(name="distractor_sales_flat", comment="Denormalized sales flat — tempting but wrong grain")
def distractor_sales_flat():
    return (
        dlt.read("silver_fact_order_line")
        .select("order_line_key", "gross_amount", "order_date_key", "product_key", "member_key")
    )


@dlt.table(name="distractor_mrr_daily", comment="Unvalidated daily MRR extract")
def distractor_mrr_daily():
    return (
        dlt.read("silver_fact_subscription_event")
        .groupBy("event_date_key")
        .agg(F.sum("mrr_delta").alias("mrr"))
    )


@dlt.table(name="distractor_workouts_raw", comment="Raw workouts alias")
def distractor_workouts_raw():
    return dlt.read("silver_fact_usage_event")


@dlt.table(name="distractor_returns_old", comment="Old returns schema")
def distractor_returns_old():
    return dlt.read("silver_fact_return").select(
        F.col("return_key").alias("rma_id"),
        F.col("return_amount").alias("amt"),
        "product_key",
    )


@dlt.table(name="distractor_campaign_spend", comment="Campaign spend stub (noise)")
def distractor_campaign_spend():
    return dlt.read("silver_dim_campaign").select(
        "campaign_key",
        F.lit(1000.0).alias("spend_usd"),
        "campaign_code",
    )


@dlt.table(name="distractor_geo_iso", comment="Geo ISO remap noise")
def distractor_geo_iso():
    return dlt.read("silver_dim_geo").select("geo_key", "country_code", "region_code")


@dlt.table(name="distractor_product_catalog_ext", comment="Extended product catalog noise")
def distractor_product_catalog_ext():
    return dlt.read("silver_dim_product").select("product_key", "sku_code", "list_price", "product_family")


@dlt.table(name="distractor_member_pii_shadow", comment="Shadow PII table — should not be used")
def distractor_member_pii_shadow():
    return dlt.read("silver_dim_member").select("member_key", "display_handle", "signup_date")


@dlt.table(name="distractor_fx_rates", comment="FX rates stub")
def distractor_fx_rates():
    return (
        dlt.read("silver_dim_date")
        .select("date_key", F.lit("USD").alias("ccy"), F.lit(1.0).alias("rate"))
        .limit(1000)
    )


@dlt.table(name="distractor_fiscal_alt", comment="Conflicting fiscal calendar")
def distractor_fiscal_alt():
    return dlt.read("silver_dim_date").select(
        "date_key",
        "calendar_date",
        F.month("calendar_date").alias("fiscal_month"),
        F.quarter("calendar_date").alias("fiscal_quarter"),
        F.year("calendar_date").alias("fiscal_year"),
    )


@dlt.table(name="distractor_nps_scores", comment="NPS noise table")
def distractor_nps_scores():
    return dlt.read("silver_dim_member").select(
        "member_key",
        (F.col("member_key") % 10).alias("nps"),
    )


@dlt.table(name="distractor_support_tickets", comment="Support tickets noise")
def distractor_support_tickets():
    return dlt.read("silver_dim_member").select(
        F.col("member_key").alias("ticket_member"),
        (F.col("member_key") % 100).alias("ticket_count"),
    )
