"""SQL helpers compatible with current Databricks SDK (no execute_and_wait)."""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


TERMINAL = {
    StatementState.SUCCEEDED,
    StatementState.FAILED,
    StatementState.CANCELED,
    StatementState.CLOSED,
}


def execute_sql(
    w: WorkspaceClient,
    warehouse_id: str,
    statement: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    wait_timeout: str = "50s",
    poll_seconds: float = 2.0,
    max_wait_seconds: float = 600.0,
) -> Any:
    resp = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout=wait_timeout,
    )
    statement_id = resp.statement_id
    state = resp.status.state if resp.status else None
    deadline = time.time() + max_wait_seconds
    while state not in TERMINAL and time.time() < deadline:
        time.sleep(poll_seconds)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state if resp.status else None

    if state != StatementState.SUCCEEDED:
        error = None
        if resp.status and resp.status.error:
            error = getattr(resp.status.error, "message", None) or str(resp.status.error)
        raise RuntimeError(f"SQL failed (state={state}): {error or statement[:200]}")
    return resp


def statement_to_df(resp: Any) -> pd.DataFrame:
    try:
        cols = [c.name for c in resp.manifest.schema.columns]  # type: ignore[union-attr]
    except Exception:
        return pd.DataFrame()
    data = []
    if resp.result and resp.result.data_array:
        data = resp.result.data_array
    return pd.DataFrame(data or [], columns=cols)


def execute_sql_df(
    w: WorkspaceClient,
    warehouse_id: str,
    statement: str,
    **kwargs: Any,
) -> pd.DataFrame:
    return statement_to_df(execute_sql(w, warehouse_id, statement, **kwargs))


def pick_warehouse_id(w: WorkspaceClient, preferred: str | None = None) -> str:
    if preferred:
        return preferred
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses available")
    # Prefer serverless / small running, else first
    for wh in warehouses:
        name = (wh.name or "").lower()
        if "serverless" in name:
            return wh.id  # type: ignore[return-value]
    return warehouses[0].id  # type: ignore[return-value]
