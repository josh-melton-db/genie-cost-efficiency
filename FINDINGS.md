# Findings — Genie context strategies and LLM cost

## Goal

Compare **Genie context philosophies** on Genie LLM cost for the same questions:

> **$ per answer = Genie LLM $ / answers**

Genie $ comes from `system.billing.usage` with `billing_origin_product = 'GENIE'`, attributed per tier via dedicated service principals. SQL warehouse cost is intentionally excluded so the benchmark isolates **context's impact on LLM cost**.

## Headline

Across **five independent repeats** of the same 10-question bank, Genie LLM **$ / answer** varied by **~3.8×** (**$0.013–$0.048** mean).

- **Lowest cost:** trusted Unity Catalog SQL functions — **$0.013 ± $0.001** /answer
- **Best prompt-only path:** ~10 focused example SQLs — **$0.027 ± $0.001** /answer
- **Reusable semantic foundation:** rich Metric Views alone — **$0.041 ± $0.001** /answer

Efficiency work should prioritize context design: fewer tokens, clearer answer shapes, and less reasoning per question.

## Leaderboard (mean Genie $/answer ± SE)

Costs are Genie LLM only. Mean and SE are over five repeats. Absolute dollars depend on region and list price; **rankings and relative gaps** are the portable result.

| Rank | Strategy | Philosophy | Mean $/answer | SE | Min–max |
|-----:|----------|------------|--------------:|---:|---------|
| 1 | Trusted UC functions | Encapsulation | **$0.0126** | ±$0.0005 | 0.0107–0.0131 |
| 2 | 10 focused examples | Encapsulation | $0.0272 | ±$0.0008 | 0.0258–0.0303 |
| 3 | SQL-source conformed Metric View | Logic in source | $0.0327 | ±$0.0002 | 0.0320–0.0332 |
| 4 | Full curation | Kitchen sink | $0.0330 | ±$0.0004 | 0.0322–0.0345 |
| 5 | MV + examples + trusted + entity | Hybrid | $0.0347 | ±$0.0024 | 0.0293–0.0416 |
| 6 | Wide pre-bucketed Metric View | Wide measures | $0.0386 | ±$0.0003 | 0.0379–0.0391 |
| 7 | Metric Views + NL instructions | Elaboration | $0.0387 | ±$0.0001 | 0.0386–0.0388 |
| 8 | Window-measure Metric Views | Semantic layer | $0.0406 | ±$0.0005 | 0.0393–0.0419 |
| 9 | Metric Views only | Semantic layer | $0.0409 | ±$0.0012 | 0.0373–0.0437 |
| 10 | MV + expressions + joins | Elaboration | $0.0483 | ±$0.0033 | 0.0424–0.0603 |

## How the methods differ

The ten configurations fall into three families.

**Encapsulation** hands Genie a near-complete answer shape. Trusted UC functions register parameterized SQL that Genie selects and fills in. Focused examples supply question→SQL pairs Genie can pattern-match. Both minimize generation and reasoning; they used the fewest Genie DBUs and the most stable $/answer across repeats.

**Semantic layer** exposes certified Metric Views and lets Genie compose `MEASURE()` queries. The Metric-Views-only configuration includes relative-period flags, synonyms, filtered/composed measures (for example hardware revenue), and a membership view for churn, MRR, and active members. Variants add window measures, push logic into a SQL-query view source, or pre-bucket period amounts into dedicated measures.

**Elaboration / hybrid** layers more prompt context on the semantic layer: natural-language instructions, SQL expression snippets and join specs, or combinations of examples, trusted assets, and entity matching. Full curation enables every lever at once.

## What drives cost

Because this benchmark measures only Genie LLM cost, **token and reasoning volume are the cost driver**. Encapsulation wins by pre-specifying the answer shape. Extra instructions, join specs, wide measure surfaces, and overloaded hybrids increase Genie DBUs without improving the answer path once the space is already well curated.

In short: **more context is not better.** Past the point of disambiguation, it is pure cost.

## Guidance for Databricks customers

1. **Invest in the Metric View first.** Put fiscal logic, revenue definitions, and membership/churn into certified semantics with synonyms and formats. That work is reusable across users and questions.
2. **Encapsulate frequent or hard questions** with a small set of trusted UC functions or focused examples. That is the fastest way to cut Genie LLM cost — and the most stable under repeat measurement.
3. **Avoid context overload.** Stacking every curation lever raises Genie spend ~1.5–4× in this benchmark.
4. **Tune context directly** if the goal is lower Genie LLM spend; warehouse tuning answers a different cost question.
5. **Be honest about modeling effort.** Relative-period flags and membership logic are real curator work — the win is that the effort is *governed and reusable*, not that it is free.

## Semantic foundation used in the benchmark

Shared Metric View building blocks included a fiscal-period anchor, a rich sales Metric View (synonyms, relative-period flags, filtered and composed measures), a membership Metric View, and optional conformed / windowed variants for alternate modeling philosophies.

## Method notes

- Same 10-question bank asked five times per tier via the Conversation API.
- Tiers use dedicated service principals so Genie LLM usage is list-billable and attributable.
- Genie billing is hour-bucketed; multi-run attribution pro-rates each hour’s DBUs by ask count.
- Wave outputs: [`results/repeat5_leaderboard.json`](results/repeat5_leaderboard.json), [`results/repeat5_per_run.json`](results/repeat5_per_run.json).
