# Genie Cost Efficiency

Benchmark Genie space **context strategies** on the same data and questions, then rank them by:

> **$ per correct answer = Genie LLM $ / correct answers**

When accuracy is held constant, cost differences come from *how* you curate the space — not from warehouse size.

**Headline result:** ten distinct curation approaches reached **100% accuracy** on the same 10-question bank, with **~$0.011–$0.044 Genie LLM cost per correct answer** (~4.1× spread). See [`FINDINGS.md`](FINDINGS.md).

---

## Why this matters

This benchmark intentionally focuses on the **Genie LLM cost plane**: billable DBUs from
`system.billing.usage` where `billing_origin_product = 'GENIE'`.

SQL warehouse usage is real customer spend, but it is deliberately excluded here so the comparison
isolates the cost impact of **context design**: how much Genie has to read, reason over, and
generate for each answer.

---

## Customer playbook

1. **Model first** — put proprietary definitions (fiscal periods, revenue recognition, membership/churn) into certified **Metric Views** with synonyms, formats, and filtered/composed measures.
2. **Encapsulate the hot path** — for frequent or hard questions, add a small set of **trusted UC SQL functions** or **focused example SQLs**.
3. **Stop there** — stacking instructions + expressions + joins + examples + functions “just in case” raises Genie cost without improving accuracy once quality is already high.

| Approach | What it is | Role |
|----------|------------|------|
| Trusted UC functions | Genie calls parameterized SQL functions | Lowest Genie $/correct among perfect configs |
| Focused examples (~10) | Question → SQL exemplars | Best prompt-only thrift |
| Rich Metric Views only | Certified semantics, no prompt soup | Strong reusable foundation; reaches 100% without prompt soup |
| Full / overloaded context | Every lever at once | Same accuracy, higher Genie $ |

---

## What’s in this repo

- **PulseForge** synthetic connected-fitness dataset (original schema; anti-memorization)
- Lakeflow Spark Declarative Pipelines (bronze → silver → gold)
- Certified Metric Views (sales, membership, conformed / windowed variants)
- Genie space tier configs (ablations + high-quality comparison set)
- Conversation API harness, execution-match scoring, and Genie LLM cost attribution from system tables
- Lakeview dashboard asset for TCO rollups

Agent mode is out of scope (UI-only; incompatible with service-principal-per-tier LLM attribution).

---

## Perfect-accuracy leaderboard (summary)

All rows below are **100% correct** on the same questions. Full narrative and method notes: [`FINDINGS.md`](FINDINGS.md).

| Rank | Strategy | Genie $/correct |
|-----:|----------|----------:|
| 1 | Trusted UC functions | **$0.011** |
| 2 | 10 focused examples | $0.027 |
| 3 | MV + examples + trusted + entity | $0.032 |
| 4–5 | SQL-source MV / full curation | ~$0.035 |
| 6–8 | Window measures / MV instructions / wide MV | ~$0.037–0.040 |
| 9–10 | Metric Views only / MV + expressions + joins | ~$0.044 |

List prices and region affect absolute dollars; **rankings** are the portable takeaway.

---

## Quick start

```bash
export DATABRICKS_CONFIG_PROFILE=<PROFILE>   # never auto-select
export CATALOG=<your_catalog>
export SCHEMA=geniebench

pip install -e .

# Provision a wave and run the question bank
WAVE=demo_subset ./scripts/run_scale_tiers.sh

# Score → attribute cost → roll up
python -m genie_bench.eval.evaluate --catalog "$CATALOG" --schema "$SCHEMA"
python -m genie_bench.cost.attribute_costs --catalog "$CATALOG" --schema "$SCHEMA"
python -m genie_bench.report.build_results_tables --catalog "$CATALOG" --schema "$SCHEMA"
```

Primary ranking column: `cost_per_correct_usd` in `metric_tco` (Genie-only; prefer tiers above the quality floor).

### Prerequisites

- Databricks CLI ≥ 0.292, authenticated profile
- Rights to create UC objects, serverless SQL warehouses, Genie spaces, and service principals
- Python 3.10+

Service principals are used so Genie LLM usage is fully list-billable (no free monthly allowance), which makes $/correct comparable across tiers.

---

## Repository layout

```
config/                 # benchmark, question bank, golden SQL, tier YAML
src/genie_bench/
  data_gen/             # synthetic data generator
  pipeline/             # SDP bronze/silver/gold
  metric_views/         # certified Metric View specs
  spaces/               # compile + provision Genie spaces
  runner/               # Conversation API harness
  eval/                 # scoring
  cost/                 # Genie LLM attribution
  report/               # metric_tco rollups
scripts/                # wave runners and helpers
resources/              # Databricks Asset Bundle defs
dashboards/             # Lakeview TCO dashboard
FINDINGS.md             # results and guidance
```

---

## Notes

- Billing system tables can lag; re-run `attribute_costs` if Genie $ shows as zero shortly after a run.
- Some Genie settings may still need UI confirmation — see `src/genie_bench/spaces/manual_steps.md`.
- Absolute dollar amounts depend on region/list price; use relative rankings when comparing strategies.
