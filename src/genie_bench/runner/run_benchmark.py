"""
Run the question bank against each provisioned Genie tier as that tier's SP.

Writes rows to {catalog}.{schema}.fact_benchmark_answer.
Applies regenerate-until-correct (max K) when a scorer is available;
otherwise logs attempts for offline eval.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from genie_bench.sql_exec import execute_sql, execute_sql_df

from genie_bench.config_utils import CONFIG_DIR, REPO_ROOT, load_benchmark_config, load_yaml, render_template
from genie_bench.spaces.sp_auth import workspace_client_for_tier

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"


def _parse_tiers(tiers: list[str] | None, tiers_csv: str | None) -> list[str]:
    if tiers_csv:
        return [t.strip().lower() for t in tiers_csv.split(",") if t.strip()]
    if tiers:
        return [t.lower() for t in tiers]
    return ["t0", "t4"]


def _ensure_results_table(w: WorkspaceClient, warehouse_id: str, fq_table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {fq_table} (
      run_id STRING,
      tier STRING,
      question_id STRING,
      attempt INT,
      space_id STRING,
      warehouse_id STRING,
      run_as_sp STRING,
      conversation_id STRING,
      message_id STRING,
      question_text STRING,
      generated_sql STRING,
      status STRING,
      clarification_asked BOOLEAN,
      row_count INT,
      latency_ms DOUBLE,
      text_response STRING,
      error STRING,
      correct BOOLEAN,
      answer_score DOUBLE,
      failure_type STRING,
      is_first_pass_correct BOOLEAN,
      regenerations_until_correct INT,
      started_at TIMESTAMP,
      finished_at TIMESTAMP
    ) USING DELTA
    """
    execute_sql(w, warehouse_id, ddl)


def _ask_genie(
    w: WorkspaceClient,
    space_id: str,
    question: str,
    conversation_id: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    """Call Genie Conversation API via REST."""
    start = time.time()
    if conversation_id:
        path = f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
        body = {"content": question}
    else:
        path = f"/api/2.0/genie/spaces/{space_id}/start-conversation"
        body = {"content": question}

    resp = w.api_client.do("POST", path, body=body)
    conversation_id = resp.get("conversation_id") or conversation_id
    message_id = resp.get("message_id") or (resp.get("message") or {}).get("id")

    # Poll for completion
    status = "IN_PROGRESS"
    message: dict[str, Any] = {}
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        message = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        status = message.get("status") or message.get("message", {}).get("status") or "UNKNOWN"
        if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            break
        time.sleep(2)

    latency_ms = (time.time() - start) * 1000
    attachments = message.get("attachments") or message.get("message", {}).get("attachments") or []
    sql = None
    text_response = message.get("content") or message.get("message", {}).get("content")
    row_count = None
    for att in attachments:
        if isinstance(att, dict):
            if "query" in att:
                sql = att["query"].get("query") if isinstance(att["query"], dict) else att.get("query")
            if "query" in att and isinstance(att["query"], dict):
                sql = sql or att["query"].get("query")
                text_response = text_response or att["query"].get("description")
            if "text" in att:
                text_response = text_response or att["text"].get("content")

    clarification = bool(text_response) and not sql and status == "COMPLETED"
    return {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "status": status if time.time() < deadline else "TIMEOUT",
        "sql": sql,
        "text_response": text_response,
        "row_count": row_count,
        "latency_ms": latency_ms,
        "clarification_asked": clarification,
        "error": message.get("error") if status == "FAILED" else None,
    }


def _escape(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def _insert_row(w: WorkspaceClient, warehouse_id: str, fq_table: str, row: dict[str, Any]) -> None:
    import base64

    cols = [
        "run_id",
        "tier",
        "question_id",
        "attempt",
        "space_id",
        "warehouse_id",
        "run_as_sp",
        "conversation_id",
        "message_id",
        "question_text",
        "generated_sql",
        "status",
        "clarification_asked",
        "row_count",
        "latency_ms",
        "text_response",
        "error",
        "correct",
        "answer_score",
        "failure_type",
        "is_first_pass_correct",
        "regenerations_until_correct",
        "started_at",
        "finished_at",
    ]
    # Store SQL as base64 to avoid quote-stripping through SQL string literals
    row = dict(row)
    if row.get("generated_sql"):
        row["generated_sql"] = "b64:" + base64.b64encode(str(row["generated_sql"]).encode()).decode()
    values = []
    for c in cols:
        v = row.get(c)
        if v is None:
            values.append("NULL")
        elif isinstance(v, bool):
            values.append("TRUE" if v else "FALSE")
        elif isinstance(v, (int, float)):
            values.append(str(v))
        else:
            values.append(_escape(str(v)))
    sql = f"INSERT INTO {fq_table} ({', '.join(cols)}) VALUES ({', '.join(values)})"
    execute_sql(w, warehouse_id, sql)


def run_benchmark(
    tiers: list[str],
    catalog: str,
    schema: str,
    max_regens: int = 3,
    timeout_seconds: int = 180,
    run_id: str | None = None,
) -> str:
    w = WorkspaceClient()
    state = json.loads(STATE_PATH.read_text())
    bank = load_yaml(CONFIG_DIR / "question_bank.yaml")
    questions = bank["questions"]
    run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    eval_wh = state.get("warehouse_id_eval") or next(iter(state["tiers"].values()))["warehouse_id"]
    fq_table = f"{catalog}.{schema}.fact_benchmark_answer"
    _ensure_results_table(w, eval_wh, fq_table)

    # Record experiment run
    execute_sql(
        w,
        eval_wh,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.experiment_runs (
          run_id STRING, started_at TIMESTAMP, tiers STRING, scale_profile STRING, notes STRING
        ) USING DELTA
        """,
    )
    cfg = load_benchmark_config()
    execute_sql(
        w,
        eval_wh,
        (
            f"INSERT INTO {catalog}.{schema}.experiment_runs VALUES ("
            f"{_escape(run_id)}, current_timestamp(), {_escape(','.join(tiers))}, "
            f"{_escape(cfg['scale_profile'])}, {_escape('standard mode')})"
        ),
    )

    for t in tiers:
        info = state["tiers"][t]
        space_id = info["space_id"]
        wh_id = info["warehouse_id"]
        sp_w, run_as = workspace_client_for_tier(w, info, fallback_to_admin=True)
        sp = info.get("sp_application_id") or run_as
        print(f"=== Tier {t} space={space_id} run_as={run_as} ===")

        for q in questions:
            qid = q["id"]
            prompt = q["prompt"]
            print(f"  {qid}: {prompt[:80]}...")
            conversation_id = None
            first_pass_correct = None
            regens = 0
            correct = None

            for attempt in range(0, max_regens + 1):
                started = datetime.now(timezone.utc).isoformat()
                try:
                    result = _ask_genie(sp_w, space_id, prompt, conversation_id, timeout_seconds)
                except Exception as e:  # noqa: BLE001
                    result = {
                        "conversation_id": conversation_id,
                        "message_id": None,
                        "status": "FAILED",
                        "sql": None,
                        "text_response": None,
                        "row_count": None,
                        "latency_ms": None,
                        "clarification_asked": False,
                        "error": str(e),
                    }
                finished = datetime.now(timezone.utc).isoformat()
                conversation_id = result.get("conversation_id")

                row = {
                    "run_id": run_id,
                    "tier": t.upper() if len(t) <= 2 else t,
                    "question_id": qid,
                    "attempt": attempt,
                    "space_id": space_id,
                    "warehouse_id": wh_id,
                    "run_as_sp": sp,
                    "conversation_id": result.get("conversation_id"),
                    "message_id": result.get("message_id"),
                    "question_text": prompt,
                    "generated_sql": result.get("sql"),
                    "status": result.get("status"),
                    "clarification_asked": result.get("clarification_asked"),
                    "row_count": result.get("row_count"),
                    "latency_ms": result.get("latency_ms"),
                    "text_response": (result.get("text_response") or "")[:10000],
                    "error": result.get("error"),
                    "correct": correct,
                    "answer_score": None,
                    "failure_type": None,
                    "is_first_pass_correct": first_pass_correct,
                    "regenerations_until_correct": regens,
                    "started_at": started,
                    "finished_at": finished,
                }
                # Normalize tier label
                row["tier"] = info["tier"]
                _insert_row(w, eval_wh, fq_table, row)

                # Offline eval fills correct later; stop early only if SQL present on first pass for edge Q9
                if attempt == 0 and result.get("status") == "COMPLETED" and result.get("sql"):
                    # Leave correct null — evaluate.py fills it
                    break
                if attempt == 0 and result.get("clarification_asked") and qid == "Q4":
                    break
                # For regenerations: ask again in same conversation
                if attempt < max_regens:
                    prompt = f"Please try again. Previous attempt failed or looked wrong. Question: {q['prompt']}"
                    regens += 1
                else:
                    break

    print(f"Run complete: {run_id}")
    try:
        from genie_bench.run_ids import save_run_id

        save_run_id(run_id)
    except Exception as e:  # noqa: BLE001
        print(f"Could not persist run_id file: {e}")
    return run_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiers", nargs="*", default=None)
    parser.add_argument("--tiers-csv", default=None, help="Comma-separated tiers, e.g. t0,t4,t16")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    runner_cfg = cfg.get("runner", {})
    run_benchmark(
        _parse_tiers(args.tiers, args.tiers_csv),
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        max_regens=int(runner_cfg.get("max_regenerations", 3)),
        timeout_seconds=int(runner_cfg.get("timeout_seconds", 180)),
        run_id=args.run_id,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
