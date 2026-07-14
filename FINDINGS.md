# Findings — Genie context strategies at equal accuracy

## Goal

Hold **answer quality fixed** and compare **Genie context philosophies** on cost:

> **$ per correct answer = Genie LLM $ / correct answers**

Genie $ comes from `system.billing.usage` with `billing_origin_product = 'GENIE'`, attributed per tier via dedicated service principals. SQL warehouse cost is intentionally excluded so the benchmark isolates **context's impact on LLM cost**.

## Headline

Across **five independent repeats** of the same 10-question bank, ten curation strategies stayed at or near perfect accuracy, while Genie LLM **$ / correct** still varied by **~3.8×** (**$0.013–$0.050** mean over four settled cost repeats).

- **Lowest cost (stable):** trusted Unity Catalog SQL functions — **$0.013 ± $0.000** /correct
- **Best prompt-only path:** ~10 focused example SQLs — **$0.027 ± $0.001** /correct
- **Reusable semantic foundation:** rich Metric Views alone — **$0.043 ± $0.001** /correct (98% mean accuracy over 5 runs)

Repeated runs confirm the single-run ranking story: encapsulation beats prompt soup, and adjacent mid-table gaps are often smaller than run-to-run noise.

## Five-run wave

| | |
|--|--|
| Ask window | `2026-07-14T13:42Z` – `16:32Z` |
| Run IDs | `run_20260714T134205Z_1bbc5f08`, `…898efcaa`, `…b981227f`, `…ee214060`, `…778af1f3` |
| Accuracy | all **5** repeats |
| Cost | **4** repeats with settled Genie billing (hours `13:00`–`15:00` UTC). Run 5 cost omitted pending hour-`16` settlement (~99% of that run’s asks). |
| Attribution | Hour-bucket Genie DBUs pro-rated to runs by ask count (`scripts/attribute_repeat_wave.py`) |

### Accuracy across 5 repeats

| Tier | Strategy | Mean accuracy | Notes |
|------|----------|--------------:|-------|
| T17 | Trusted UC functions | **100%** | 5/5 perfect |
| T16 | 10 focused examples | **100%** | 5/5 |
| T4 | Full curation | **100%** | 5/5 |
| T24 | SQL-source conformed MV | **100%** | 5/5 |
| T21 | MV + examples + trusted + entity | **100%** | 5/5 |
| T25 | Wide pre-bucketed MV | **100%** | 5/5 |
| T19 | MV + NL instructions | **100%** | 5/5 |
| T20 | MV + expressions + joins | **100%** | 5/5 |
| T26 | Window-measure MVs | **98%** | one 9/10 run |
| T3 | Metric Views only | **98%** | one 9/10 run |

## Leaderboard (mean Genie $/correct ± SE)

Costs are Genie LLM only. Mean / SE over the **4 settled cost repeats**. Absolute dollars depend on region and list price; **rankings and relative gaps** are the portable result.

| Rank | Strategy | Philosophy | Mean $/correct | SE | Min–max | Mean acc |
|-----:|----------|------------|---------------:|---:|---------|----------|
| 1 | Trusted UC functions | Encapsulation | **$0.0130** | ±$0.0000 | 0.0130–0.0131 | 100% |
| 2 | 10 focused examples | Encapsulation | $0.0273 | ±$0.0010 | 0.0258–0.0303 | 100% |
| 3 | Full curation | Kitchen sink | $0.0327 | ±$0.0002 | 0.0322–0.0330 | 100% |
| 4 | SQL-source conformed Metric View | Logic in source | $0.0329 | ±$0.0002 | 0.0326–0.0332 | 100% |
| 5 | MV + examples + trusted + entity | Hybrid | $0.0360 | ±$0.0026 | 0.0316–0.0416 | 100% |
| 6 | Wide pre-bucketed Metric View | Wide measures | $0.0385 | ±$0.0003 | 0.0379–0.0390 | 100% |
| 7 | Metric Views + NL instructions | Elaboration | $0.0386 | ±$0.0001 | 0.0386–0.0388 | 100% |
| 8 | Window-measure Metric Views | Semantic layer | $0.0422 | ±$0.0016 | 0.0400–0.0466 | 98% |
| 9 | Metric Views only | Semantic layer | $0.0425 | ±$0.0005 | 0.0414–0.0437 | 98% |
| 10 | MV + expressions + joins | Elaboration | $0.0497 | ±$0.0038 | 0.0427–0.0603 | 100% |

### What the repeats change vs a single run

- **Trusted functions and focused examples stay #1 / #2** with tiny standard errors — those gaps are real.
- **Full curation sits mid-table**, not at the top of cost: kitchen-sink context is not free, but it is cheaper here than MV-only or expression/join elaboration.
- **Metric Views only and MV + expressions/joins** remain the expensive end; T20’s SE is the largest, so treat fine mid-pack orderings as soft.
- **Report bands** (encapsulation / mid / expensive) rather than over-interpreting adjacent ranks with overlapping ranges.

## How the methods differ

The ten configurations fall into three families.

**Encapsulation** hands Genie a near-complete answer shape. Trusted UC functions register parameterized SQL that Genie selects and fills in. Focused examples supply question→SQL pairs Genie can pattern-match. Both minimize generation and reasoning; they used the fewest Genie DBUs and the most stable $/correct across repeats.

**Semantic layer** exposes certified Metric Views and lets Genie compose `MEASURE()` queries. The Metric-Views-only configuration includes relative-period flags, synonyms, filtered/composed measures (for example hardware revenue), and a membership view for churn, MRR, and active members. Variants add window measures, push logic into a SQL-query view source, or pre-bucket period amounts into dedicated measures.

**Elaboration / hybrid** layers more prompt context on the semantic layer: natural-language instructions, SQL expression snippets and join specs, or combinations of examples, trusted assets, and entity matching. Full curation enables every lever at once — useful for accuracy insurance, costly versus thin encapsulation once accuracy is already high.

## What drives cost

Because this benchmark measures only Genie LLM cost, **token and reasoning volume are the cost driver**. Encapsulation wins by pre-specifying the answer shape. Extra instructions, join specs, wide measure surfaces, and overloaded hybrids increase Genie DBUs **without raising accuracy** once the bar is already ~100%.

In short: **more context is not better.** Past the point of disambiguation, it is pure cost.

## Guidance for Databricks customers

1. **Invest in the Metric View first.** Put fiscal logic, revenue definitions, and membership/churn into certified semantics with synonyms and formats. That work is reusable across users and questions, and a well-modeled semantic layer alone can reach high accuracy at competitive $/correct.
2. **Encapsulate frequent or hard questions** with a small set of trusted UC functions or focused examples. That is the fastest way to cut Genie LLM cost once quality is high — and the most stable under repeat measurement.
3. **Avoid context overload.** Stacking every curation lever raises Genie spend ~1.5–4× with no accuracy gain in this benchmark.
4. **Tune context directly** if the goal is lower Genie LLM spend; warehouse tuning answers a different cost question.
5. **Be honest about modeling effort.** Relative-period flags and membership logic are real curator work — the win is that the effort is *governed and reusable*, not that it is free.

## Semantic foundation used in the benchmark

Shared Metric View building blocks included a fiscal-period anchor, a rich sales Metric View (synonyms, relative-period flags, filtered and composed measures), a membership Metric View, and optional conformed / windowed variants for alternate modeling philosophies.

## Method notes

- Questions are scored primarily by golden-SQL **execution match** (plus lightweight rubrics for ambiguous / exploratory prompts).
- Tiers use dedicated service principals so Genie LLM usage is list-billable and attributable.
- Genie billing is **hour-bucketed**; multi-run attribution pro-rates each hour’s DBUs by ask count. Billing tables can lag several hours — Genie $ of zero shortly after a run usually means settlement delay, not free usage.
- Machine-readable wave outputs: [`results/repeat5_leaderboard.json`](results/repeat5_leaderboard.json), [`results/repeat5_per_run.json`](results/repeat5_per_run.json).
