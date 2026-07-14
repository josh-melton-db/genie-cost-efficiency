#!/usr/bin/env bash
# Post-benchmark: evaluate → contamination → costs → metrics → summary JSON
set -eu
set -o pipefail
export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:?}"
export CATALOG="${CATALOG:-demos}"
export SCHEMA="${SCHEMA:-geniebench}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH:-src}"
cd "$(dirname "$0")/.."
mkdir -p logs

RUN_ID="${1:-}"
if [[ -z "$RUN_ID" ]]; then
  RUN_ID=$(cat src/genie_bench/spaces/latest_run_id.txt)
fi
echo "Post-process run_id=$RUN_ID"

python -u -m genie_bench.eval.evaluate --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
python -u -m genie_bench.report.build_results_tables --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
python -u scripts/contamination_check.py --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA" || true
python -u -m genie_bench.cost.attribute_costs --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA" || true
python -u -m genie_bench.report.build_results_tables --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"

python -u <<PY
import json
from pathlib import Path
from databricks.sdk import WorkspaceClient
from genie_bench.sql_exec import execute_sql

run_id = "$RUN_ID"
state = json.loads(Path("src/genie_bench/spaces/provisioned_state.json").read_text())
w = WorkspaceClient()
wh = state["warehouse_id_eval"]
cat, sch = state["catalog"], state["schema"]
sql = f"""
SELECT
  m.tier, m.axis, m.lever, m.tier_name,
  m.first_pass_accuracy, m.eventual_accuracy,
  m.n_correct, m.n_questions,
  m.bytes_scanned, m.sql_statements, m.bytes_per_correct,
  m.genie_dbus, m.warehouse_dbus, m.total_cost_usd, m.cost_per_correct_usd,
  m.above_quality_floor
FROM {cat}.{sch}.metric_tco m
WHERE m.run_id = '{run_id}'
ORDER BY m.first_pass_accuracy DESC, m.bytes_scanned ASC
"""
res = execute_sql(w, wh, sql)
cols = [c.name for c in res.manifest.schema.columns]
rows = [dict(zip(cols, r)) for r in (res.result.data_array or [])]

# Per-question matrix for narrative
sql2 = f"""
SELECT tier, question_id,
  COALESCE(is_first_pass_correct, correct) AS first_pass,
  correct, failure_type, status
FROM {cat}.{sch}.fact_benchmark_answer
WHERE run_id = '{run_id}'
QUALIFY ROW_NUMBER() OVER (PARTITION BY tier, question_id ORDER BY attempt DESC) = 1
ORDER BY tier, question_id
"""
res2 = execute_sql(w, wh, sql2)
cols2 = [c.name for c in res2.manifest.schema.columns]
matrix = [dict(zip(cols2, r)) for r in (res2.result.data_array or [])]

out = {"run_id": run_id, "tiers": rows, "matrix": matrix}
Path("logs/latest_tco_summary.json").write_text(json.dumps(out, indent=2, default=str))
print(json.dumps({"run_id": run_id, "n_tiers": len(rows), "top": rows[:5]}, indent=2, default=str))
PY
echo "Wrote logs/latest_tco_summary.json"
