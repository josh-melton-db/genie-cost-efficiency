"""
Generate PulseForge connected-fitness synthetic data with dbldatagen.

Writes partitioned Parquet into a UC Volume for Auto Loader ingestion.
Volumes are controlled by config/benchmark.yaml scale_profiles.
Planted signals (June device launch, churn-reduction campaign) are deterministic
given the configured seed.

Run (Databricks Connect serverless or notebook):
  python -m genie_bench.data_gen.generate_raw
  # or: databricks bundle run generate_raw_data -t demo --profile <PROFILE>
"""

from __future__ import annotations

import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from genie_bench.config_utils import load_benchmark_config, volume_path

# Avoid importing heavy Spark deps at module import for unit-light tooling
try:
    import dbldatagen as dg
    from faker import Faker
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:  # pragma: no cover - local lint without Spark
    dg = None  # type: ignore
    F = None  # type: ignore
    SparkSession = None  # type: ignore


REGIONS = [
    ("NA", "North America"),
    ("EMEA", "Europe, Middle East & Africa"),
    ("APAC", "Asia Pacific"),
    ("LATAM", "Latin America"),
]

PRODUCT_FAMILIES = [
    ("PulseForge X1", "DEVICE"),
    ("PulseForge Bike", "DEVICE"),
    ("PulseForge Band", "DEVICE"),
    ("HeartLink Strap", "ACCESSORY"),
    ("PowerPad Mat", "ACCESSORY"),
    ("ForgeAll Access", "SUBSCRIPTION"),
    ("ForgePlus Monthly", "SUBSCRIPTION"),
]

MEMBER_TIERS = ["Spark", "Volt", "Forge", "EliteForge"]
LINE_STATUSES = ["FULFILLED", "FULFILLED", "FULFILLED", "FULFILLED", "CANCELLED", "RETURNED"]
SUB_EVENTS = ["ACTIVATE", "RENEW", "RENEW", "RENEW", "CANCEL"]


def _volumes(cfg: dict) -> dict[str, int]:
    profile = cfg["scale_profiles"][cfg["scale_profile"]]
    sf = float(profile["scale_factor"])
    base = cfg["base_volumes"]

    def n(key: str) -> int:
        return max(1, int(base[key] * sf))

    usage = n("usage_events") if profile.get("include_usage_events", True) else 0
    if profile.get("usage_events_override") is not None:
        usage = int(profile["usage_events_override"])
    return {
        "members": n("members"),
        "skus": max(10, n("skus")),
        "campaigns": max(5, n("campaigns")),
        "geos": min(200, max(8, n("geos"))),
        "orders": n("orders"),
        "subscription_events": n("subscription_events"),
        "usage_events": usage,
        "returns_rate": float(base["returns_rate"]),
        "order_lines_per_order": float(base["order_lines_per_order"]),
        "fiscal_years": int(base["fiscal_years"]),
        "scale_factor": sf,
    }


def _get_spark() -> "SparkSession":
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless(True).getOrCreate()
    except Exception:
        return SparkSession.builder.appName("genie-tco-data-gen").getOrCreate()


def build_dim_date(spark: "SparkSession", fiscal_years: int, seed: int):
    """Bespoke 4-4-5 fiscal calendar (not Gregorian month semantics)."""
    # Fiscal year starts first Monday of February for originality
    end = date.today()
    start = date(end.year - fiscal_years, 2, 1)
    # Align start to Monday
    start = start - timedelta(days=start.weekday())
    rows = []
    d = start
    # 4-4-5 weeks per quarter
    week_pattern = [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]
    fiscal_year = start.year
    fiscal_month = 1
    fiscal_quarter = 1
    week_in_month = 0
    weeks_this_month = week_pattern[0]
    date_key = 1
    while d <= end + timedelta(days=30):
        rows.append(
            (
                date_key,
                d,
                fiscal_year,
                fiscal_month,
                fiscal_quarter,
                d.isocalendar()[1],
                d.strftime("%Y-%m-%d"),
            )
        )
        date_key += 1
        d += timedelta(days=1)
        if d.weekday() == 0:  # new week
            week_in_month += 1
            if week_in_month >= weeks_this_month:
                fiscal_month += 1
                week_in_month = 0
                if fiscal_month > 12:
                    fiscal_month = 1
                    fiscal_year += 1
                fiscal_quarter = (fiscal_month - 1) // 3 + 1
                weeks_this_month = week_pattern[fiscal_month - 1]

    schema = StructType(
        [
            StructField("date_key", IntegerType()),
            StructField("calendar_date", DateType()),
            StructField("fiscal_year", IntegerType()),
            StructField("fiscal_month", IntegerType()),
            StructField("fiscal_quarter", IntegerType()),
            StructField("iso_week", IntegerType()),
            StructField("date_label", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def build_dim_geo(spark: "SparkSession", n_geos: int, seed: int):
    fake = Faker()
    Faker.seed(seed)
    rows = []
    for i in range(n_geos):
        region_code, region_name = REGIONS[i % len(REGIONS)]
        rows.append(
            (
                i + 1,
                f"GEO-{i+1:04d}",
                fake.city(),
                fake.country_code(),
                region_code,
                region_name,
            )
        )
    schema = StructType(
        [
            StructField("geo_key", IntegerType()),
            StructField("geo_code", StringType()),
            StructField("city_name", StringType()),
            StructField("country_code", StringType()),
            StructField("region_code", StringType()),
            StructField("region_name", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def build_dim_product(spark: "SparkSession", n_skus: int, seed: int):
    fake = Faker()
    Faker.seed(seed + 1)
    rows = []
    for i in range(n_skus):
        family, pclass = PRODUCT_FAMILIES[i % len(PRODUCT_FAMILIES)]
        # Ensure PulseForge X1 exists as sku 0 family for planted signal
        if i == 0:
            family, pclass = "PulseForge X1", "DEVICE"
        list_price = {
            "DEVICE": fake.pyfloat(min_value=800, max_value=3500, right_digits=2),
            "ACCESSORY": fake.pyfloat(min_value=20, max_value=250, right_digits=2),
            "SUBSCRIPTION": fake.pyfloat(min_value=12, max_value=60, right_digits=2),
        }[pclass]
        rows.append(
            (
                i + 1,
                f"SKU-{i+1:06d}",
                f"{family} {fake.color_name()}",
                family,
                pclass,
                float(list_price),
            )
        )
    # Guarantee nonexistent code is absent (Q9)
    schema = StructType(
        [
            StructField("product_key", IntegerType()),
            StructField("sku_code", StringType()),
            StructField("product_name", StringType()),
            StructField("product_family", StringType()),
            StructField("product_class", StringType()),
            StructField("list_price", DoubleType()),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def build_dim_campaign(spark: "SparkSession", n_campaigns: int, seed: int, signals: dict):
    fake = Faker()
    Faker.seed(seed + 2)
    rows = []
    launch = signals["june_device_launch"]["campaign_code"]
    retain = signals["churn_reduction_campaign"]["campaign_code"]
    for i in range(n_campaigns):
        code = f"CMP-{i+1:04d}"
        name = fake.catch_phrase()
        if i == 0:
            code, name = launch, "PulseForge X1 Launch Blitz"
        elif i == 1:
            code, name = retain, "Retain Plus Q2 Winback"
        rows.append((i + 1, code, name, ["AWARENESS", "CONVERSION", "RETENTION"][i % 3]))
    schema = StructType(
        [
            StructField("campaign_key", IntegerType()),
            StructField("campaign_code", StringType()),
            StructField("campaign_name", StringType()),
            StructField("campaign_objective", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def build_dim_member(spark: "SparkSession", n_members: int, n_geos: int, seed: int):
    assert dg is not None
    fake = Faker()
    Faker.seed(seed + 3)
    # Small label pool via Faker, then dbldatagen for scale
    handles = [f"forge_{fake.user_name()}_{i}" for i in range(min(5000, n_members))]
    dataspec = (
        dg.DataGenerator(spark, name="dim_member", rows=n_members, randomSeed=seed)
        .withColumn("member_key", "long", minValue=1, maxValue=n_members, uniqueValues=n_members, step=1)
        .withColumn("member_id", "string", prefix="MBR-", uniqueValues=n_members)
        .withColumn("display_handle", "string", values=handles, random=True)
        .withColumn("member_tier", "string", values=MEMBER_TIERS, random=True)
        .withColumn("home_geo_key", "int", minValue=1, maxValue=n_geos, random=True)
        .withColumn("signup_date", "date", begin="2022-01-01", end="2025-12-31", random=True)
    )
    return dataspec.build()


def build_fact_orders(
    spark: "SparkSession",
    n_orders: int,
    n_members: int,
    n_geos: int,
    n_campaigns: int,
    date_df,
    seed: int,
    signals: dict,
):
    assert dg is not None
    min_dk = date_df.agg(F.min("date_key")).collect()[0][0]
    max_dk = date_df.agg(F.max("date_key")).collect()[0][0]

    dataspec = (
        dg.DataGenerator(spark, name="fact_order", rows=n_orders, randomSeed=seed)
        .withColumn("order_key", "long", minValue=1, maxValue=n_orders, step=1)
        .withColumn("order_id", "string", prefix="ORD-", uniqueValues=min(n_orders, 100000))
        .withColumn("member_key", "long", minValue=1, maxValue=n_members, random=True)
        .withColumn("geo_key", "int", minValue=1, maxValue=n_geos, random=True)
        .withColumn("campaign_key", "int", minValue=1, maxValue=n_campaigns, random=True)
        .withColumn("order_date_key", "int", minValue=int(min_dk), maxValue=int(max_dk), random=True)
        .withColumn("ship_date_offset", "int", minValue=0, maxValue=7, random=True)
        .withColumn("recognition_date_offset", "int", minValue=0, maxValue=14, random=True)
        .withColumn("order_gross_amount", "double", minValue=15.0, maxValue=4000.0, random=True)
        .withColumn("order_status", "string", values=["OPEN", "SHIPPED", "FULFILLED", "CANCELLED"], random=True)
    )
    orders = dataspec.build()
    orders = (
        orders.withColumn("ship_date_key", F.col("order_date_key") + F.col("ship_date_offset"))
        .withColumn(
            "recognition_date_key",
            F.col("order_date_key") + F.col("recognition_date_offset"),
        )
        .drop("ship_date_offset", "recognition_date_offset")
    )

    # Plant June spike via join (avoids collecting large key lists)
    june = date_df.filter(
        (F.col("fiscal_year") == signals["june_device_launch"]["fiscal_year"])
        & (F.col("fiscal_month") == signals["june_device_launch"]["month"])
    ).select(F.col("date_key").alias("june_dk"))
    mult = float(signals["june_device_launch"]["revenue_multiplier"])
    orders = (
        orders.join(june, orders.order_date_key == june.june_dk, "left")
        .withColumn(
            "order_gross_amount",
            F.when(
                (F.col("june_dk").isNotNull()) & (F.col("campaign_key") == 1),
                F.col("order_gross_amount") * F.lit(mult),
            ).otherwise(F.col("order_gross_amount")),
        )
        .drop("june_dk")
    )
    return orders


def build_fact_order_lines(
    spark: "SparkSession",
    orders_df,
    n_skus: int,
    lines_per_order: float,
    seed: int,
    signals: dict,
    date_df,
    n_orders: int,
):
    assert dg is not None
    n_lines = max(n_orders, int(n_orders * lines_per_order))
    dataspec = (
        dg.DataGenerator(spark, name="fact_order_line", rows=n_lines, randomSeed=seed + 10)
        .withColumn("order_line_key", "long", minValue=1, maxValue=n_lines, step=1)
        .withColumn("order_key", "long", minValue=1, maxValue=n_orders, random=True)
        .withColumn("product_key", "int", minValue=1, maxValue=n_skus, random=True)
        .withColumn("qty", "int", minValue=1, maxValue=3, random=True)
        .withColumn("unit_price", "double", minValue=10.0, maxValue=3500.0, random=True)
        .withColumn("line_status", "string", values=LINE_STATUSES, random=True)
        .withColumn("return_amount", "double", minValue=0.0, maxValue=100.0, random=True)
    )
    lines = dataspec.build()
    lines = (
        lines.join(
            orders_df.select(
                "order_key",
                "member_key",
                "geo_key",
                "campaign_key",
                "order_date_key",
                "ship_date_key",
                "recognition_date_key",
            ),
            on="order_key",
            how="left",
        )
        .withColumn("gross_amount", F.col("qty") * F.col("unit_price"))
        .withColumn(
            "net_recognized_amount",
            F.when(F.col("line_status") == "FULFILLED", F.col("gross_amount") - F.col("return_amount")).otherwise(
                F.lit(0.0)
            ),
        )
    )

    june = date_df.filter(
        (F.col("fiscal_year") == signals["june_device_launch"]["fiscal_year"])
        & (F.col("fiscal_month") == signals["june_device_launch"]["month"])
    ).select(F.col("date_key").alias("june_dk"))
    mult = float(signals["june_device_launch"]["revenue_multiplier"])
    lines = (
        lines.join(june, lines.recognition_date_key == june.june_dk, "left")
        .withColumn(
            "net_recognized_amount",
            F.when(
                (F.col("product_key") == 1) & (F.col("june_dk").isNotNull()),
                F.col("net_recognized_amount") * F.lit(mult),
            ).otherwise(F.col("net_recognized_amount")),
        )
        .drop("june_dk")
    )
    return lines


def build_fact_subscription_events(
    spark: "SparkSession",
    n_events: int,
    n_members: int,
    n_campaigns: int,
    date_df,
    seed: int,
    signals: dict,
):
    assert dg is not None
    min_dk = date_df.agg(F.min("date_key")).collect()[0][0]
    max_dk = date_df.agg(F.max("date_key")).collect()[0][0]
    dataspec = (
        dg.DataGenerator(spark, name="fact_subscription_event", rows=n_events, randomSeed=seed + 20)
        .withColumn("subscription_event_key", "long", minValue=1, maxValue=n_events, step=1)
        .withColumn("member_key", "long", minValue=1, maxValue=n_members, random=True)
        .withColumn("event_type", "string", values=SUB_EVENTS, random=True)
        .withColumn("event_date_key", "int", minValue=int(min_dk), maxValue=int(max_dk), random=True)
        .withColumn("mrr_delta", "double", minValue=-60.0, maxValue=60.0, random=True)
        .withColumn("attribution_campaign_key", "int", minValue=1, maxValue=n_campaigns, random=True)
    )
    events = dataspec.build()
    retain = signals["churn_reduction_campaign"]
    window = date_df.filter(
        (F.col("calendar_date") >= F.lit(retain["start_date"]))
        & (F.col("calendar_date") <= F.lit(retain["end_date"]))
    ).select(F.col("date_key").alias("win_dk"))
    events = (
        events.join(window, events.event_date_key == window.win_dk, "left")
        .withColumn(
            "event_type",
            F.when(
                (F.col("attribution_campaign_key") == 2)
                & (F.col("win_dk").isNotNull())
                & (F.col("event_type") == "CANCEL")
                & (F.col("subscription_event_key") % 3 == 0),
                F.lit("RENEW"),
            ).otherwise(F.col("event_type")),
        )
        .drop("win_dk")
    )
    return events


def build_fact_usage_events(
    spark: "SparkSession",
    n_events: int,
    n_members: int,
    n_skus: int,
    date_df,
    seed: int,
):
    if n_events <= 0:
        return None
    assert dg is not None
    min_dk = date_df.agg(F.min("date_key")).collect()[0][0]
    max_dk = date_df.agg(F.max("date_key")).collect()[0][0]
    dataspec = (
        dg.DataGenerator(spark, name="fact_usage_event", rows=n_events, randomSeed=seed + 30)
        .withColumn("usage_event_key", "long", minValue=1, maxValue=n_events, step=1)
        .withColumn("member_key", "long", minValue=1, maxValue=n_members, random=True)
        .withColumn("product_key", "int", minValue=1, maxValue=n_skus, random=True)
        .withColumn("event_date_key", "int", minValue=int(min_dk), maxValue=int(max_dk), random=True)
        .withColumn("duration_sec", "int", minValue=60, maxValue=7200, random=True)
        .withColumn("calories", "int", minValue=20, maxValue=1200, random=True)
        .withColumn("workout_type", "string", values=["RIDE", "RUN", "STRENGTH", "YOGA", "ROW"], random=True)
    )
    return dataspec.build()


def build_fact_return(spark: "SparkSession", order_lines_df, returns_rate: float, seed: int):
    # Sample a fraction of lines as returns
    returns = (
        order_lines_df.filter(F.col("line_status").isin("RETURNED", "FULFILLED"))
        .sample(False, returns_rate, seed=seed)
        .select(
            F.monotonically_increasing_id().alias("return_key"),
            "order_line_key",
            "order_key",
            "member_key",
            "product_key",
            "return_amount",
            F.col("recognition_date_key").alias("return_date_key"),
        )
    )
    return returns


def write_partitioned(df, path: str, partition_col: str | None = None, known_rows: int | None = None):
    writer = df.write.mode("overwrite").format("parquet")
    # Avoid high-cardinality partition explosion on raw files; single directory is fine for Auto Loader
    writer.save(path)
    n = known_rows if known_rows is not None else -1
    print(f"Wrote {n if n >= 0 else '(uncounted)'} rows -> {path}", flush=True)


def generate(cfg: dict | None = None) -> None:
    cfg = cfg or load_benchmark_config()
    spark = _get_spark()
    vols = _volumes(cfg)
    seed = int(cfg["seed"])
    signals = cfg["signals"]
    catalog, schema = cfg["catalog"], cfg["schema"]
    root = volume_path(catalog, schema, cfg.get("raw_volume_name", "raw") if "raw_volume_name" in cfg else "raw")
    # Prefer bundle var name
    root = volume_path(catalog, schema, "raw")

    print(f"Generating PulseForge data profile={cfg['scale_profile']} volumes={vols}")
    print(f"Output volume root: {root}")

    dim_date = build_dim_date(spark, vols["fiscal_years"], seed)
    dim_geo = build_dim_geo(spark, vols["geos"], seed)
    dim_product = build_dim_product(spark, vols["skus"], seed)
    dim_campaign = build_dim_campaign(spark, vols["campaigns"], seed, signals)
    dim_member = build_dim_member(spark, vols["members"], vols["geos"], seed)

    write_partitioned(dim_date, f"{root}/dim_date", known_rows=dim_date.count())
    write_partitioned(dim_geo, f"{root}/dim_geo", known_rows=vols["geos"])
    write_partitioned(dim_product, f"{root}/dim_product", known_rows=vols["skus"])
    write_partitioned(dim_campaign, f"{root}/dim_campaign", known_rows=vols["campaigns"])
    write_partitioned(dim_member, f"{root}/dim_member", known_rows=vols["members"])

    print("Building fact_order...", flush=True)
    fact_order = build_fact_orders(
        spark,
        vols["orders"],
        vols["members"],
        vols["geos"],
        vols["campaigns"],
        dim_date,
        seed,
        signals,
    )
    write_partitioned(fact_order, f"{root}/fact_order", known_rows=vols["orders"])

    print("Building fact_order_line...", flush=True)
    fact_order_line = build_fact_order_lines(
        spark,
        fact_order,
        vols["skus"],
        vols["order_lines_per_order"],
        seed,
        signals,
        dim_date,
        vols["orders"],
    )
    n_lines = max(vols["orders"], int(vols["orders"] * vols["order_lines_per_order"]))
    write_partitioned(fact_order_line, f"{root}/fact_order_line", known_rows=n_lines)

    print("Building fact_subscription_event...", flush=True)
    fact_sub = build_fact_subscription_events(
        spark,
        vols["subscription_events"],
        vols["members"],
        vols["campaigns"],
        dim_date,
        seed,
        signals,
    )
    write_partitioned(fact_sub, f"{root}/fact_subscription_event", known_rows=vols["subscription_events"])

    print("Building fact_usage_event...", flush=True)
    fact_usage = build_fact_usage_events(
        spark, vols["usage_events"], vols["members"], vols["skus"], dim_date, seed
    )
    if fact_usage is None:
        from pyspark.sql.types import LongType, IntegerType, StringType, StructField, StructType

        empty_schema = StructType(
            [
                StructField("usage_event_key", LongType()),
                StructField("member_key", LongType()),
                StructField("product_key", IntegerType()),
                StructField("event_date_key", IntegerType()),
                StructField("duration_sec", IntegerType()),
                StructField("calories", IntegerType()),
                StructField("workout_type", StringType()),
            ]
        )
        fact_usage = spark.createDataFrame([], empty_schema)
    write_partitioned(fact_usage, f"{root}/fact_usage_event", known_rows=vols["usage_events"])

    print("Building fact_return...", flush=True)
    fact_return = build_fact_return(spark, fact_order_line, vols["returns_rate"], seed)
    write_partitioned(fact_return, f"{root}/fact_return")

    manifest = spark.createDataFrame(
        [
            (
                cfg["scale_profile"],
                float(vols["scale_factor"]),
                seed,
                datetime.utcnow().isoformat(),
                vols["members"],
                vols["orders"],
                vols["usage_events"],
            )
        ],
        "scale_profile string, scale_factor double, seed int, generated_at string, n_members long, n_orders long, n_usage_events long",
    )
    write_partitioned(manifest, f"{root}/_manifest", known_rows=1)
    print("Generation complete.", flush=True)


def main(argv: list[str] | None = None) -> int:
    generate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
