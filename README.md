# Genie Cost Efficiency

Benchmark Genie space **context strategies** on the same data and questions, then rank them by:

> **$ per answer = Genie LLM $ / answers**

Cost differences come from *how* you curate the space — not from warehouse size.

**Headline result:** five repeats of ten curation approaches show Genie LLM **$ / answer** spanning **~$0.013–$0.048** (~3.8×). See [`FINDINGS.md`](FINDINGS.md).

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
3. **Stop there** — stacking instructions + expressions + joins + examples + functions “just in case” raises Genie cost once the space is already well curated.

| Approach | What it is | Role |
|----------|------------|------|
| Trusted UC functions | Genie calls parameterized SQL functions | Lowest Genie $/answer |
| Focused examples (~10) | Question → SQL exemplars | Best prompt-only thrift |
| Rich Metric Views only | Certified semantics, no prompt soup | Strong reusable foundation |
| Full / overloaded context | Every lever at once | Higher Genie $ |

---

## What’s in this repo

- **PulseForge** synthetic connected-fitness dataset (original schema; anti-memorization)
- Lakeflow Spark Declarative Pipelines (bronze → silver → gold)
- Certified Metric Views (sales, membership, conformed / windowed variants)
- Genie space tier configs (ablations + high-quality comparison set)
- Conversation API harness and Genie LLM cost attribution from system tables
- Lakeview dashboard asset for TCO rollups

Agent mode is out of scope (UI-only; incompatible with service-principal-per-tier LLM attribution).

---

## Leaderboard (summary)

Mean Genie $/answer over **five repeats** (± SE). Full narrative: [`FINDINGS.md`](FINDINGS.md).

| Rank | Strategy | Mean $/answer | SE |
|-----:|----------|--------------:|---:|
| 1 | Trusted UC functions | **$0.013** | ±$0.001 |
| 2 | 10 focused examples | $0.027 | ±$0.001 |
| 3–4 | SQL-source MV / full curation | ~$0.033 | ±$0.000 |
| 5–7 | Hybrid / wide MV / MV+instructions | ~$0.035–0.039 | |
| 8–9 | Window MVs / Metric Views only | ~$0.041 | |
| 10 | MV + expressions + joins | $0.048 | ±$0.003 |

List prices and region affect absolute dollars; **rankings and relative gaps** are the portable takeaway.

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

Primary ranking column: Genie LLM $ per answer (see `results/repeat5_leaderboard.json`).

### Prerequisites

- Databricks CLI ≥ 0.292, authenticated profile
- Rights to create UC objects, serverless SQL warehouses, Genie spaces, and service principals
- Python 3.10+

Service principals are used so Genie LLM usage is fully list-billable (no free monthly allowance), which makes $/answer comparable across tiers.

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
results/                # published wave leaderboards
resources/              # Databricks Asset Bundle defs
dashboards/             # Lakeview TCO dashboard
FINDINGS.md             # results and guidance
```

---

## Notes

- Billing system tables can lag; re-run attribution if Genie $ shows as zero shortly after a run.
- Some Genie settings may still need UI confirmation — see `src/genie_bench/spaces/manual_steps.md`.
- Absolute dollar amounts depend on region/list price; use relative rankings when comparing strategies.
