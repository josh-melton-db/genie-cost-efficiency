#!/usr/bin/env python3
"""Create catalog, schema, raw volume, and results scaffolding."""

from __future__ import annotations

import argparse
import sys

from databricks.sdk import WorkspaceClient

from genie_bench.sql_exec import execute_sql, pick_warehouse_id


def bootstrap(catalog: str, schema: str, volume: str = "raw", secrets_scope: str = "genie-tco-bench") -> None:
    w = WorkspaceClient()
    wh = pick_warehouse_id(w)
    stmts = [
        f"CREATE CATALOG IF NOT EXISTS {catalog}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}",
    ]
    for s in stmts:
        print(f"Running: {s}")
        try:
            execute_sql(w, wh, s)
        except Exception as e:
            # catalog may already exist / no create privilege — continue if schema/volume succeed
            print(f"Note: {e}")

    try:
        w.secrets.create_scope(scope=secrets_scope)
        print(f"Created secrets scope {secrets_scope}")
    except Exception as e:
        print(f"Secrets scope note: {e}")

    print(f"Bootstrap complete: {catalog}.{schema}, volume={volume}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--volume", default="raw")
    args = p.parse_args(argv)
    bootstrap(args.catalog, args.schema, args.volume)
    return 0


if __name__ == "__main__":
    sys.exit(main())
