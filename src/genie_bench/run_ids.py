"""Persist / resolve benchmark run_ids for local CLI and Databricks jobs."""

from __future__ import annotations

from pathlib import Path

from genie_bench.config_utils import REPO_ROOT

RUN_ID_FILE = REPO_ROOT / "src" / "genie_bench" / "spaces" / "latest_run_id.txt"


def save_run_id(run_id: str) -> None:
    RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
    RUN_ID_FILE.write_text(run_id)
    print(f"Saved run_id locally -> {RUN_ID_FILE}")


def load_run_id() -> str:
    if RUN_ID_FILE.exists():
        return RUN_ID_FILE.read_text().strip()
    raise FileNotFoundError(
        f"No run_id at {RUN_ID_FILE}. Pass --run-id or run the benchmark first."
    )


def load_run_id_from_uc(catalog: str, schema: str, warehouse_id: str) -> str:
    """Fallback for job compute: latest row in experiment_runs."""
    from databricks.sdk import WorkspaceClient

    from genie_bench.sql_exec import execute_sql

    w = WorkspaceClient()
    sql = f"""
    SELECT run_id FROM {catalog}.{schema}.experiment_runs
    ORDER BY started_at DESC
    LIMIT 1
    """
    res = execute_sql(w, warehouse_id, sql)
    rows = res.result.data_array if res.result else []
    if not rows:
        raise FileNotFoundError("No experiment_runs rows found")
    return rows[0][0]
