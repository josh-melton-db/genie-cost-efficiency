#!/usr/bin/env bash
# Overnight TCO story pipeline: fix-provision → ask → eval → costs → metrics dump.
# Avoid process substitution under nohup — it can kill the job silently.
set -eu
set -o pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
export CATALOG="${CATALOG:-demos}"
export SCHEMA="${SCHEMA:-geniebench}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH:-src}"
cd "$(dirname "$0")/.."

mkdir -p logs
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG="logs/overnight_tco_${STAMP}.log"
echo "Logging to $LOG"
exec >>"$LOG" 2>&1

echo "=== Overnight TCO story @ $STAMP profile=$PROFILE ==="

FIX_TIERS=(t3 t4 t17)
STORY_TIERS=(t0 t1 t3 t4 t5 t8 t9 t15 t16 t17 t22)

echo "=== Rebuild TVFs ==="
python -u -m genie_bench.spaces.build_sql_functions --catalog "$CATALOG" --schema "$SCHEMA"

echo "=== Recompile + provision fixed tiers: ${FIX_TIERS[*]} ==="
python -u -m genie_bench.spaces.compile_space --tiers "${FIX_TIERS[@]}" --catalog "$CATALOG" --schema "$SCHEMA"
python -u -m genie_bench.spaces.provision_spaces --tiers "${FIX_TIERS[@]}" --catalog "$CATALOG" --schema "$SCHEMA"

echo "=== Run benchmark: ${STORY_TIERS[*]} ==="
python -u -m genie_bench.runner.run_benchmark --tiers "${STORY_TIERS[@]}" --catalog "$CATALOG" --schema "$SCHEMA"

RUN_ID=$(cat src/genie_bench/spaces/latest_run_id.txt)
echo "=== Run complete: $RUN_ID ==="
echo "$RUN_ID" > logs/latest_run_id.txt

echo "=== Evaluate ==="
python -u -m genie_bench.eval.evaluate --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"

echo "=== Contamination check ==="
python -u scripts/contamination_check.py --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA" || true

echo "=== Attribute costs (may still be \$0 if billing unsettled) ==="
python -u -m genie_bench.cost.attribute_costs --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA" || true

echo "=== Build results tables ==="
python -u -m genie_bench.report.build_results_tables --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"

echo "=== Dump shareable summary JSON ==="
python -u <<'PY'
import json
from pathlib import Path
from databricks.sdk import WorkspaceClient
from genie_bench.sql_exec import execute_sql

run_id = Path("src/genie_bench/spaces/latest_run_id.txt").read_text().strip()
state = json.loads(Path("src/genie_bench/spaces/provisioned_state.json").read_text())
w = WorkspaceClient()
wh = state["warehouse_id_eval"]
cat, sch = state["catalog"], state["schema"]
sql = f"""
SELECT
  m.tier,
  COALESCE(m.axis, d.axis, 'unknown') AS axis,
  COALESCE(m.lever, d.lever, m.tier) AS lever,
  m.first_pass_accuracy,
  m.eventual_accuracy,
  m.n_correct,
  m.n_questions,
  m.bytes_scanned,
  m.sql_statements,
  m.bytes_per_correct,
  m.genie_dbus,
  m.warehouse_dbus,
  m.total_cost_usd,
  m.cost_per_correct_usd,
  m.above_quality_floor
FROM {cat}.{sch}.metric_tco m
LEFT JOIN {cat}.{sch}.dim_tier d ON m.tier = d.tier
WHERE m.run_id = '{run_id}'
ORDER BY m.first_pass_accuracy DESC, m.bytes_scanned ASC
"""
res = execute_sql(w, wh, sql)
cols = [c.name for c in res.manifest.schema.columns]
rows = [dict(zip(cols, r)) for r in (res.result.data_array or [])]
out = {"run_id": run_id, "tiers": rows}
Path("logs/latest_tco_summary.json").write_text(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str))
PY

echo "=== DONE run_id=$RUN_ID log=$LOG ==="
touch logs/overnight_DONE
