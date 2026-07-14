"""Create/replace certified PulseForge metric views from YAML definitions.

Creates v_fiscal_anchor first (relative-period helper), then all metric views.
Preview-channel warehouses (DBR 18.1+) are required for one-to-many joins and
window measures (mv_pulseforge_conformed_wide, mv_pulseforge_sales_windowed).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from genie_bench.sql_exec import execute_sql

from genie_bench.config_utils import load_benchmark_config, render_template

MV_DIR = Path(__file__).parent

# (uc_leaf_name, yaml_path, short_comment)
MV_SPECS = [
    ("mv_pulseforge_sales_basic", MV_DIR / "mv_sales_basic.yaml", "PulseForge basic sales KPIs"),
    ("mv_pulseforge_sales_rich", MV_DIR / "mv_sales_rich.yaml", "PulseForge rich governed sales metrics"),
    ("mv_pulseforge_membership", MV_DIR / "mv_membership.yaml", "PulseForge membership / subscription metrics"),
    (
        "mv_pulseforge_conformed_sqlsrc",
        MV_DIR / "mv_conformed_sqlsrc.yaml",
        "PulseForge conformed KPIs (SQL-query source)",
    ),
    (
        "mv_pulseforge_conformed_wide",
        MV_DIR / "mv_conformed_wide.yaml",
        "PulseForge wide pre-bucketed sales KPIs (OTM stand-in)",
    ),
    (
        "mv_pulseforge_sales_windowed",
        MV_DIR / "mv_sales_windowed.yaml",
        "PulseForge sales metrics with window measures",
    ),
]


def _sql_create(full_name: str, yaml_body: str, comment: str) -> str:
    return f"""
CREATE OR REPLACE VIEW {full_name}
WITH METRICS
LANGUAGE YAML
COMMENT '{comment.replace("'", "''")}'
AS $$
{yaml_body}
$$
"""


def _create_fiscal_anchor(w: WorkspaceClient, catalog: str, schema: str, warehouse_id: str | None) -> None:
    path = MV_DIR / "v_fiscal_anchor.sql"
    sql = render_template(path.read_text(), {"catalog": catalog, "schema": schema})
    full_name = f"{catalog}.{schema}.v_fiscal_anchor"
    print(f"Creating {full_name} ...")
    if warehouse_id:
        execute_sql(w, warehouse_id, sql)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        spark.sql(sql)
    tag_sql = f"ALTER VIEW {full_name} SET TAGS ('certified' = 'true')"
    try:
        if warehouse_id:
            execute_sql(w, warehouse_id, tag_sql)
        else:
            spark.sql(tag_sql)  # type: ignore[name-defined]
    except Exception as e:  # noqa: BLE001
        print(f"Warning: could not tag {full_name} certified: {e}")
    print(f"OK {full_name}")


def _create_wide_helpers(w: WorkspaceClient, catalog: str, schema: str, warehouse_id: str | None) -> None:
    """UC helper views required by mv_pulseforge_conformed_wide (one-to-many)."""
    path = MV_DIR / "v_wide_helpers.sql"
    sql = render_template(path.read_text(), {"catalog": catalog, "schema": schema})
    parts = [p.strip() for p in sql.split("CREATE OR REPLACE VIEW") if p.strip()]
    for part in parts:
        # Skip leading comment-only chunks
        if not part.split()[0].startswith(catalog) and f"{catalog}." not in part[:120]:
            continue
        stmt = ("CREATE OR REPLACE VIEW " + part).rstrip().rstrip(";")
        name = part.split()[0]
        print(f"Creating helper view {name} ...")
        if warehouse_id:
            execute_sql(w, warehouse_id, stmt)
        else:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            spark.sql(stmt)
        print(f"OK {name}")


def _tag_certified(w: WorkspaceClient, full_name: str, warehouse_id: str | None, spark=None) -> None:
    tag_sql = f"ALTER VIEW {full_name} SET TAGS ('certified' = 'true')"
    try:
        if warehouse_id:
            execute_sql(w, warehouse_id, tag_sql)
        elif spark is not None:
            spark.sql(tag_sql)
    except Exception as e:  # noqa: BLE001
        print(f"Warning: could not tag {full_name} certified: {e}")


def build(
    catalog: str,
    schema: str,
    warehouse_id: str | None = None,
    only: list[str] | None = None,
) -> None:
    w = WorkspaceClient()
    mapping = {"catalog": catalog, "schema": schema}
    spark = None

    _create_fiscal_anchor(w, catalog, schema, warehouse_id)
    _create_wide_helpers(w, catalog, schema, warehouse_id)

    wanted = None
    if only:
        wanted = {n.lower().removeprefix("mv_pulseforge_").removeprefix("mv_") for n in only}
        wanted |= {n.lower() for n in only}

    for name, path, comment in MV_SPECS:
        leaf_key = name.removeprefix("mv_pulseforge_")
        if wanted is not None and name.lower() not in wanted and leaf_key.lower() not in wanted:
            continue
        body = render_template(path.read_text(), mapping)
        full_name = f"{catalog}.{schema}.{name}"
        sql = _sql_create(full_name, body, comment)
        print(f"Creating {full_name} ...")
        try:
            if warehouse_id:
                execute_sql(w, warehouse_id, sql)
            else:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            # Preview features may be unavailable — continue so GA MVs still build.
            print(f"ERROR creating {full_name}: {e}")
            if name in ("mv_pulseforge_conformed_wide", "mv_pulseforge_sales_windowed"):
                print(
                    f"  (preview feature — ensure warehouse channel=PREVIEW / DBR 18.1+; skipping {name})"
                )
                continue
            raise
        _tag_certified(w, full_name, warehouse_id, spark)
        print(f"OK {full_name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Optional subset of MV leaf names (e.g. sales_rich membership)",
    )
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    catalog = args.catalog or cfg["catalog"]
    schema = args.schema or cfg["schema"]
    build(catalog, schema, args.warehouse_id, only=args.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())
