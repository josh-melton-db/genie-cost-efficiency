"""
Evaluate Genie answers vs golden SQL result sets + rubrics.

Uses a neutral eval warehouse (not a tier warehouse) so scoring SQL cost
does not contaminate tier warehouse attribution.

Optionally logs to MLflow when DATABRICKS tracking is configured.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from databricks.sdk import WorkspaceClient
from genie_bench.sql_exec import execute_sql, execute_sql_df

from genie_bench.config_utils import CONFIG_DIR, REPO_ROOT, load_benchmark_config, load_yaml, render_template
from genie_bench.eval.scorers import ScoreResult, classify_failure, execution_match, llm_judge_heuristic

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"


def _fetch_df(w: WorkspaceClient, warehouse_id: str, sql: str) -> pd.DataFrame:
    res = execute_sql(w, warehouse_id, sql)
    # Convert SDK result to DataFrame
    try:
        manifest = res.manifest
        columns = [c.name for c in manifest.schema.columns]  # type: ignore
        data_array = res.result.data_array if res.result else []  # type: ignore
        return pd.DataFrame(data_array or [], columns=columns)
    except Exception:
        # Fallback empty
        return pd.DataFrame()


def _load_golden_sql(qid: str, catalog: str, schema: str) -> str:
    bank = load_yaml(CONFIG_DIR / "question_bank.yaml")
    q = next(x for x in bank["questions"] if x["id"] == qid)
    sql_path = q["golden_sql"]
    return render_template((REPO_ROOT / sql_path).read_text(), {"catalog": catalog, "schema": schema})


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"true", "1", "t", "yes"}


def _rehydrate_sql(w: WorkspaceClient, space_id: str, conversation_id: str, message_id: str) -> str | None:
    """Fetch SQL from Genie API — avoids quote-stripping from SQL INSERT storage."""
    if not space_id or not conversation_id or not message_id:
        return None
    try:
        message = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
    except Exception:
        return None
    for att in message.get("attachments") or []:
        if isinstance(att, dict) and isinstance(att.get("query"), dict):
            q = att["query"].get("query")
            if q:
                return q
    return None


def evaluate_run(run_id: str, catalog: str, schema: str) -> None:
    w = WorkspaceClient()
    state = json.loads(STATE_PATH.read_text())
    eval_wh = state["warehouse_id_eval"]
    answers = load_yaml(CONFIG_DIR / "golden" / "answers.yaml")
    defaults = answers.get("defaults", {})
    per_q = answers.get("per_question", {})
    fq = f"{catalog}.{schema}.fact_benchmark_answer"

    rows = _fetch_df(
        w,
        eval_wh,
        f"""
        SELECT * FROM {fq}
        WHERE run_id = '{run_id}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tier, question_id ORDER BY attempt DESC) = 1
        """,
    )
    if rows.empty:
        rows = _fetch_df(w, eval_wh, f"SELECT * FROM {fq} WHERE run_id = '{run_id}'")
        if not rows.empty:
            rows = rows.sort_values("attempt").groupby(["tier", "question_id"], as_index=False).tail(1)

    print(f"Evaluating {len(rows)} answer rows for run {run_id}")

    mlflow_run = None
    try:
        import mlflow

        mlflow.set_experiment("/Shared/genie-tco-benchmark")
        mlflow_run = mlflow.start_run(run_name=run_id)
    except Exception as e:  # noqa: BLE001
        print(f"MLflow unavailable ({e}); continuing without tracking")

    updates = []
    for _, row in rows.iterrows():
        qid = row["question_id"]
        cfg_q = per_q.get(qid, {})
        scoring = cfg_q.get("scoring")
        bank_q = next(x for x in load_yaml(CONFIG_DIR / "question_bank.yaml")["questions"] if x["id"] == qid)
        scoring = scoring or bank_q.get("scoring", "execution_match")

        genie_sql = _rehydrate_sql(
            w,
            str(row.get("space_id") or ""),
            str(row.get("conversation_id") or ""),
            str(row.get("message_id") or ""),
        )
        if not genie_sql:
            genie_sql = row.get("generated_sql")
            if genie_sql is not None:
                genie_sql = str(genie_sql).strip() or None
                if genie_sql and genie_sql.startswith("b64:"):
                    import base64

                    try:
                        genie_sql = base64.b64decode(genie_sql[4:]).decode()
                    except Exception:  # noqa: BLE001
                        pass
        status = row.get("status")
        clarification = _as_bool(row.get("clarification_asked"))

        if scoring == "llm_judge":
            # Prefer Genie attachment text description when available
            text = None
            try:
                message = w.api_client.do(
                    "GET",
                    f"/api/2.0/genie/spaces/{row['space_id']}/conversations/{row['conversation_id']}/messages/{row['message_id']}",
                )
                for att in message.get("attachments") or []:
                    if isinstance(att, dict) and isinstance(att.get("text"), dict):
                        text = att["text"].get("content")
                    if isinstance(att, dict) and isinstance(att.get("query"), dict):
                        text = text or att["query"].get("description")
            except Exception:
                text = row.get("text_response")
            # Include SQL in the judge blob so trusted-function / MV flag answers score correctly
            result = llm_judge_heuristic(qid, text, genie_sql)
        else:
            golden_sql = _load_golden_sql(qid if qid != "Q10" else "Q1", catalog, schema)
            golden_df = _fetch_df(w, eval_wh, golden_sql)
            genie_df = None
            sql_exec_ok = False
            if genie_sql and status == "COMPLETED":
                try:
                    genie_df = _fetch_df(w, eval_wh, genie_sql)
                    sql_exec_ok = True
                except Exception as e:  # noqa: BLE001
                    print(f"  {row['tier']} {qid}: genie SQL failed to execute: {e}")
                    genie_df = None
            if cfg_q.get("expect_zero_or_empty") and not sql_exec_ok:
                result = ScoreResult(False, 0.0, "no_sql_or_error")
            else:
                result = execution_match(
                    genie_df,
                    golden_df,
                    relative_tol=float(defaults.get("numeric_relative_tolerance", 0.001)),
                    absolute_tol=float(defaults.get("numeric_absolute_tolerance", 0.01)),
                    key_columns=cfg_q.get("key_columns"),
                    top_n=cfg_q.get("top_n"),
                    expect_zero_or_empty=bool(cfg_q.get("expect_zero_or_empty")),
                )

        failure = classify_failure(status, genie_sql, clarification, result.correct)
        updates.append(
            {
                "tier": row["tier"],
                "question_id": qid,
                "attempt": int(row["attempt"]),
                "correct": result.correct,
                "answer_score": result.answer_score,
                "failure_type": failure,
            }
        )
        print(f"  {row['tier']} {qid}: correct={result.correct} score={result.answer_score} fail={failure}")

        if mlflow_run:
            import mlflow

            mlflow.log_metric(f"{row['tier']}_{qid}_correct", 1.0 if result.correct else 0.0)
            mlflow.log_metric(f"{row['tier']}_{qid}_score", float(result.answer_score))

    for u in updates:
        sql = f"""
        UPDATE {fq}
        SET correct = {str(u['correct']).upper()},
            answer_score = {u['answer_score']},
            failure_type = {("NULL" if not u["failure_type"] else "'" + u["failure_type"] + "'")},
            is_first_pass_correct = CASE WHEN attempt = 0 THEN {str(u['correct']).upper()} ELSE is_first_pass_correct END
        WHERE run_id = '{run_id}'
          AND tier = '{u['tier']}'
          AND question_id = '{u['question_id']}'
          AND attempt = {u['attempt']}
        """
        execute_sql(w, eval_wh, sql)

    by_tier: dict[str, list[bool]] = {}
    for u in updates:
        by_tier.setdefault(u["tier"], []).append(bool(u["correct"]))
    for tier, flags in by_tier.items():
        acc = sum(flags) / max(len(flags), 1)
        print(f"Tier {tier} accuracy={acc:.2%}")

    cont = load_benchmark_config().get("contamination", {})
    t0_acc = sum(by_tier.get("T0", [])) / max(len(by_tier.get("T0", [])), 1) if "T0" in by_tier else None
    full_tier = cont.get("full_tier", "T4")
    full_acc = (
        sum(by_tier.get(full_tier, [])) / max(len(by_tier.get(full_tier, [])), 1)
        if full_tier in by_tier
        else None
    )
    if t0_acc is not None and t0_acc > float(cont.get("t0_max_first_pass_accuracy", 0.35)):
        msg = f"CONTAMINATION GUARDRAIL FAIL: T0 accuracy {t0_acc:.2%} too high"
        print(msg)
        if cont.get("fail_if_contaminated", True):
            raise SystemExit(msg)
    if t0_acc is not None and full_acc is not None:
        print(f"Contamination check: T0={t0_acc:.2%} {full_tier}={full_acc:.2%} (expect T0 weak, {full_tier} strong)")

    if mlflow_run:
        import mlflow

        mlflow.end_run()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    run_id = args.run_id.strip()
    if not run_id:
        from genie_bench.run_ids import load_run_id, load_run_id_from_uc
        import json
        from pathlib import Path
        from genie_bench.config_utils import REPO_ROOT

        try:
            run_id = load_run_id()
        except FileNotFoundError:
            state = json.loads((REPO_ROOT / "src/genie_bench/spaces/provisioned_state.json").read_text())
            run_id = load_run_id_from_uc(
                args.catalog or cfg["catalog"],
                args.schema or cfg["schema"],
                state["warehouse_id_eval"],
            )
    evaluate_run(run_id, args.catalog or cfg["catalog"], args.schema or cfg["schema"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
