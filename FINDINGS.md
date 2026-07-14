# Findings — Genie context strategies at equal accuracy

## Goal

Hold **answer quality fixed** and compare **Genie context philosophies** on cost:

> **$ per correct answer = Genie LLM $ / correct answers**

Genie $ comes from `system.billing.usage` with `billing_origin_product = 'GENIE'`, attributed per tier via dedicated service principals. SQL warehouse cost is intentionally excluded so the benchmark isolates **context's impact on LLM cost**.

## Headline

**Ten distinct curation strategies reached 100% accuracy** on the same 10-question bank. Genie LLM cost still varied by **~4.1×** (**$0.011–$0.044 per correct answer**) in the clean settled rerun.

- **Lowest cost:** trusted Unity Catalog SQL functions (**$0.011**/correct)
- **Best prompt-only path:** ~10 focused example SQLs (**$0.027**/correct)
- **Reusable semantic foundation:** rich Metric Views alone reached 100% at **$0.044**/correct

Efficiency work should prioritize context design: fewer tokens, clearer answer shapes, and less reasoning per question.

## Latest clean rerun

Clean rerun: `run_20260713T224641Z_bd471198`

All 10 high-quality tiers reached **100% accuracy** in a single fresh run:

`T3`, `T4`, `T16`, `T17`, `T19`, `T20`, `T21`, `T24`, `T25`, `T26`

Genie billing for this ask window (`2026-07-13T22:46Z`–`23:11Z`) has settled and is reflected below. The attribution script skips writing zero-cost rows when billing has not settled.

## Perfect-accuracy leaderboard

All rows: **100% (10/10)** on the shared question bank. Costs are Genie LLM only.

| Rank | Strategy | Philosophy | Genie $ | Genie $/correct | Genie DBUs |
|-----:|----------|------------|--------:|----------------:|----------:|
| 1 | Trusted UC functions | Encapsulation | **$0.11** | **$0.011** | 1.5 |
| 2 | 10 focused examples | Encapsulation | $0.27 | $0.027 | 3.9 |
| 3 | MV + examples + trusted + entity | Hybrid | $0.32 | $0.032 | 4.5 |
| 4 | SQL-source conformed Metric View | Logic in source | $0.35 | $0.035 | 5.0 |
| 5 | Full curation | Kitchen sink | $0.35 | $0.035 | 5.0 |
| 6 | Window-measure Metric Views | Semantic layer | $0.37 | $0.037 | 5.3 |
| 7 | Metric Views + NL instructions | Elaboration | $0.39 | $0.039 | 5.6 |
| 8 | Wide pre-bucketed Metric View | Wide measures | $0.40 | $0.040 | 5.8 |
| 9 | Metric Views only | Semantic layer | $0.44 | $0.044 | 6.3 |
| 10 | MV + expressions + joins | Elaboration | $0.44 | $0.044 | 6.3 |

Absolute dollars depend on region and list price; **rankings and relative gaps** are the portable result.

## How the methods differ

The ten perfect configurations fall into three families.

**Encapsulation** hands Genie a near-complete answer shape. Trusted UC functions register parameterized SQL that Genie selects and fills in. Focused examples supply question→SQL pairs Genie can pattern-match. Both minimize generation and reasoning; they used the fewest Genie DBUs (about 1.5–4).

**Semantic layer** exposes certified Metric Views and lets Genie compose `MEASURE()` queries. The Metric-Views-only configuration includes relative-period flags, synonyms, filtered/composed measures (for example hardware revenue), and a membership view for churn, MRR, and active members. Variants add window measures, push logic into a SQL-query view source, or pre-bucket period amounts into dedicated measures.

**Elaboration / hybrid** layers more prompt context on the semantic layer: natural-language instructions, SQL expression snippets and join specs, or combinations of examples, trusted assets, and entity matching. Full curation enables every lever at once.

## What drives cost

Because this benchmark measures only Genie LLM cost, **token and reasoning volume are the cost driver**. Encapsulation wins by pre-specifying the answer shape. Extra instructions, join specs, wide measure surfaces, and “add every lever” hybrids increase Genie DBUs **without raising accuracy** once the bar is already 100%.

In short: **more context is not better.** Past the point of disambiguation, it is pure cost.

## Guidance for Databricks customers

1. **Invest in the Metric View first.** Put fiscal logic, revenue definitions, and membership/churn into certified semantics with synonyms and formats. That work is reusable across users and questions, and a well-modeled semantic layer alone can reach high accuracy at competitive $/correct.
2. **Encapsulate frequent or hard questions** with a small set of trusted UC functions or focused examples. That is the fastest way to cut Genie LLM cost once quality is high.
3. **Avoid context overload.** Stacking every curation lever raises Genie spend 1.5–4× with no accuracy gain in this benchmark.
4. **Tune context directly** if the goal is lower Genie LLM spend; warehouse tuning answers a different cost question.
5. **Be honest about modeling effort.** Relative-period flags and membership logic are real curator work — the win is that the effort is *governed and reusable*, not that it is free.

## Semantic foundation used in the benchmark

Shared Metric View building blocks included a fiscal-period anchor, a rich sales Metric View (synonyms, relative-period flags, filtered and composed measures), a membership Metric View, and optional conformed / windowed variants for alternate modeling philosophies.

## Method notes

- Questions are scored primarily by golden-SQL **execution match** (plus lightweight rubrics for ambiguous / exploratory prompts).
- Tiers use dedicated service principals so Genie LLM usage is list-billable and attributable.
- Billing tables can lag; Genie $ of zero shortly after a run usually means settlement delay, not free usage.
