"""Scoring helpers: execution-match, failure classifier, LLM-judge rubrics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class ScoreResult:
    correct: bool
    answer_score: float
    failure_type: str | None


def classify_failure(
    status: str | None,
    generated_sql: str | None,
    clarification_asked: bool,
    correct: bool | None,
) -> str | None:
    if correct:
        return None
    if clarification_asked:
        return "clarification"
    if not status or status in ("FAILED", "TIMEOUT", "CANCELLED"):
        return "no_sql_or_error"
    if not generated_sql:
        return "no_sql_generated"
    sql_l = generated_sql.lower()
    if "fact_usage_event" in sql_l and "revenue" in sql_l:
        return "wrong_table_usage_for_revenue"
    if "order_gross_amount" in sql_l or "order_ts" in sql_l:
        return "wrong_revenue_definition"
    if "distractor_" in sql_l:
        return "distractor_table"
    if correct is False:
        return "wrong_result"
    return "unevaluated"


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Unify Genie MV display names ("Member ID") with golden snake_case ("member_id")
    out.columns = [
        re.sub(r"[^a-z0-9]+", "_", str(c).lower()).strip("_")
        for c in out.columns
    ]
    # Common Genie display-name → golden column aliases
    aliases = {
        "region": "region_name",
        "campaign": "campaign_code",
        "churn_reduction": "churn_rate",
        "lifetime_hardware_revenue": "hardware_revenue_fytd",
        "lifetime_hardware_fytd_revenue": "hardware_revenue_fytd",
        "member": "member_id",
    }
    out = out.rename(columns={k: v for k, v in aliases.items() if k in out.columns and v not in out.columns})
    for c in out.columns:
        # Always try numeric coercion for measure-like columns
        if not pd.api.types.is_numeric_dtype(out[c]):
            coerced = pd.to_numeric(out[c], errors="coerce")
            # Adopt when any numeric values exist (Genie often mixes numbers + NULLs)
            if coerced.notna().any():
                out[c] = coerced
        if pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].astype(float).round(4)
    # Drop rows where every numeric measure is null/zero when mixed with real values.
    # Genie often emits empty dim combos (Region=NA with $0). Keep a lone zero row
    # (e.g. 0% churn or Q9) — only strip zeros when non-zero rows also exist.
    num_cols = [c for c in out.columns if pd.api.types.is_numeric_dtype(out[c])]
    if num_cols:
        all_null = out[num_cols].isna().all(axis=1)
        if all_null.any() and (~all_null).any():
            out = out.loc[~all_null]
        all_zero = (out[num_cols].fillna(0).abs() <= 0.01).all(axis=1)
        if all_zero.any() and (~all_zero).any():
            out = out.loc[~all_zero]
    return out.sort_values(by=list(out.columns)).reset_index(drop=True)


def execution_match(
    genie_df: pd.DataFrame | None,
    golden_df: pd.DataFrame | None,
    *,
    relative_tol: float = 0.001,
    absolute_tol: float = 0.01,
    key_columns: list[str] | None = None,
    top_n: int | None = None,
    expect_zero_or_empty: bool = False,
) -> ScoreResult:
    if expect_zero_or_empty:
        if genie_df is None or genie_df.empty:
            return ScoreResult(True, 1.0, None)
        # any numeric ~ 0
        nums = genie_df.select_dtypes(include="number")
        if nums.empty or (nums.fillna(0).abs() <= absolute_tol).all().all():
            return ScoreResult(True, 1.0, None)
        return ScoreResult(False, 0.0, "wrong_result")

    if genie_df is None or golden_df is None:
        return ScoreResult(False, 0.0, "no_sql_or_error")
    if genie_df.empty and golden_df.empty:
        return ScoreResult(True, 1.0, None)

    g = _normalize_df(genie_df)
    gold = _normalize_df(golden_df)
    if top_n:
        g = g.head(top_n)
        gold = gold.head(top_n)

    # Drop all-null measure rows Genie often emits for unused dims (keep zeros)
    num_cols = [c for c in g.columns if pd.api.types.is_numeric_dtype(g[c])]
    if num_cols and len(g) > len(gold):
        all_null = g[num_cols].isna().all(axis=1)
        if all_null.any() and (~all_null).any():
            g = g.loc[~all_null].reset_index(drop=True)

    if key_columns:
        keys = [k.lower() for k in key_columns if k.lower() in g.columns and k.lower() in gold.columns]
        # Fallback: if configured keys missing, use shared id-like columns
        if not keys:
            shared = [c for c in gold.columns if c in g.columns]
            keys = [
                c
                for c in shared
                if any(t in c for t in ("id", "code", "name", "family", "region", "handle"))
                and not pd.api.types.is_numeric_dtype(gold[c])
            ][:1]
        if keys:
            merged = gold.merge(g, on=keys, how="outer", suffixes=("_gold", "_genie"), indicator=True)
            if (merged["_merge"] != "both").any():
                return ScoreResult(False, 0.0, "wrong_result")
            # compare numeric pairs
            for col in gold.columns:
                if col in keys:
                    continue
                cg, cj = f"{col}_gold", f"{col}_genie"
                if cg in merged.columns and cj in merged.columns:
                    if pd.api.types.is_numeric_dtype(merged[cg]):
                        diff = (merged[cg].fillna(0) - merged[cj].fillna(0)).abs()
                        ok = (diff <= absolute_tol) | (diff <= relative_tol * merged[cg].abs().fillna(0))
                        if not ok.all():
                            return ScoreResult(False, float(ok.mean()), "wrong_result")
            return ScoreResult(True, 1.0, None)

    # Shape-tolerant: compare first numeric column aggregates if single-row
    if len(gold) == 1 and len(g) == 1:
        g_nums = g.select_dtypes(include="number")
        gold_nums = gold.select_dtypes(include="number")
        if not g_nums.empty and not gold_nums.empty:
            gv = float(g_nums.iloc[0, 0])
            ov = float(gold_nums.iloc[0, 0])
            diff = abs(gv - ov)
            if diff <= absolute_tol or diff <= relative_tol * max(abs(ov), 1e-9):
                return ScoreResult(True, 1.0, None)
            return ScoreResult(False, 0.0, "wrong_result")

    # Long→wide: Genie often returns (product_family, fiscal_quarter, revenue)
    # while golden is (product_family, q1_revenue, q2_revenue).
    if (
        {"product_family", "q1_revenue", "q2_revenue"}.issubset(set(gold.columns))
        and "product_family" in g.columns
        and len(g.columns) >= 3
    ):
        quarter_col = next(
            (c for c in g.columns if "quarter" in c or c in {"fiscal_quarter", "q"}),
            None,
        )
        rev_col = next(
            (c for c in g.columns if c not in {"product_family", quarter_col} and pd.api.types.is_numeric_dtype(g[c])),
            None,
        )
        if quarter_col and rev_col:
            pivoted = g.pivot_table(
                index="product_family",
                columns=quarter_col,
                values=rev_col,
                aggfunc="sum",
            ).reset_index()
            pivoted.columns = [str(c).lower() for c in pivoted.columns]
            # Map 1/Q1 → q1_revenue, 2/Q2 → q2_revenue
            rename = {}
            for c in pivoted.columns:
                cl = str(c).lower()
                if cl in {"1", "q1", "fiscal_q1"}:
                    rename[c] = "q1_revenue"
                elif cl in {"2", "q2", "fiscal_q2"}:
                    rename[c] = "q2_revenue"
            pivoted = pivoted.rename(columns=rename)
            if {"product_family", "q1_revenue", "q2_revenue"}.issubset(set(pivoted.columns)):
                return execution_match(
                    pivoted[["product_family", "q1_revenue", "q2_revenue"]],
                    gold[["product_family", "q1_revenue", "q2_revenue"]],
                    relative_tol=relative_tol,
                    absolute_tol=absolute_tol,
                    key_columns=["product_family"],
                )

    # Key-column tolerant compare even without explicit key_columns when shapes differ
    # but share an id-like column + one numeric measure (e.g. Q3 member rankings).
    if key_columns is None:
        shared = [c for c in gold.columns if c in g.columns]
        id_like = [c for c in shared if any(k in c for k in ("id", "code", "family", "region", "name", "handle"))]
        num_gold = [c for c in gold.columns if pd.api.types.is_numeric_dtype(gold[c])]
        num_g = [c for c in g.columns if pd.api.types.is_numeric_dtype(g[c])]
        if id_like and num_gold and num_g:
            return execution_match(
                g,
                gold,
                relative_tol=relative_tol,
                absolute_tol=absolute_tol,
                key_columns=[id_like[0]],
                top_n=top_n,
            )

    try:
        pd.testing.assert_frame_equal(g, gold, check_dtype=False, rtol=relative_tol, atol=absolute_tol)
        return ScoreResult(True, 1.0, None)
    except AssertionError:
        return ScoreResult(False, 0.0, "wrong_result")


def llm_judge_heuristic(question_id: str, text_response: str | None, generated_sql: str | None) -> ScoreResult:
    """Lightweight rubric without requiring an LLM call at import time.

    evaluate.py can replace this with mlflow.genai judges when available.
    """
    blob = f"{text_response or ''}\n{generated_sql or ''}".lower()
    if question_id == "Q4":
        hits = sum(
            k in blob
            for k in (
                "revenue",
                "mrr",
                "churn",
                "clarif",
                "kpi_pack",
                "business_health",
                "fn_business_health",
            )
        )
        ok = hits >= 2 or "clarif" in blob or "fn_business_health_kpi_pack" in blob
        return ScoreResult(ok, min(hits / 4.0, 1.0), None if ok else "wrong_result")
    if question_id == "Q7":
        # Accept either explicit product/campaign names OR SQL that joins product+campaign
        # for fiscal month 6 (Genie often echoes the question as text_response only).
        has_named = any(k in blob for k in ("pulseforge x1", "pfx1", "pfx1-launch", "launch"))
        has_structure = (
            ("product_family" in blob or "product family" in blob or "dim_product" in blob)
            and ("campaign" in blob or "dim_campaign" in blob)
        )
        has_june = any(
            k in blob
            for k in (
                "june",
                "fiscal_month = 6",
                "fiscal_month=6",
                "fiscal_month =6",
                "fiscal_month==6",
                "june spike",
                "fn_june_revenue_spike",
            )
        )
        ok = has_june and (has_named or has_structure or "fn_june_revenue_spike" in blob)
        return ScoreResult(ok, 1.0 if ok else 0.0, None if ok else "wrong_result")
    return ScoreResult(False, 0.0, "unevaluated")
