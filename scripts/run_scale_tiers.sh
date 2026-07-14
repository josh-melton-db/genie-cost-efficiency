#!/usr/bin/env bash
# Provision and run Genie TCO tiers in waves (workspace concurrent-warehouse limits).
# Default: full sweep t0-t23. Override with TIERS= or WAVE=demo_subset.
set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
export CATALOG="${CATALOG:-demos}"
export SCHEMA="${SCHEMA:-geniebench}"

FULL_TIERS="t0 t1 t2 t3 t4 t5 t6 t7 t8 t9 t10 t11 t12 t13 t14 t15 t16 t17 t18 t19 t20 t21 t22 t23"
DEMO_SUBSET="t0 t1 t3 t4 t5 t16 t22"
TCO_STORY="t0 t1 t3 t4 t5 t8 t9 t15 t16 t17 t22"

WAVE="${WAVE:-full}"
case "$WAVE" in
  demo_subset|demo) TIERS_LIST="${TIERS:-$DEMO_SUBSET}" ;;
  tco_story|story) TIERS_LIST="${TIERS:-$TCO_STORY}" ;;
  full|*) TIERS_LIST="${TIERS:-$FULL_TIERS}" ;;
esac

# Shell-friendly array
read -r -a TIER_ARR <<< "$TIERS_LIST"
WAVE_SIZE="${WAVE_SIZE:-6}"

echo "Profile=$PROFILE catalog=$CATALOG schema=$SCHEMA wave=$WAVE tiers=${TIER_ARR[*]}"

python -m genie_bench.spaces.build_sql_functions --catalog "$CATALOG" --schema "$SCHEMA" || true

# Provision in waves to avoid hitting concurrent warehouse creation limits
for ((i=0; i<${#TIER_ARR[@]}; i+=WAVE_SIZE)); do
  chunk=("${TIER_ARR[@]:i:WAVE_SIZE}")
  echo "=== Provisioning wave: ${chunk[*]} ==="
  python -m genie_bench.spaces.compile_space --tiers "${chunk[@]}" --catalog "$CATALOG" --schema "$SCHEMA"
  python -m genie_bench.spaces.provision_spaces --tiers "${chunk[@]}" --catalog "$CATALOG" --schema "$SCHEMA"
done

echo "=== Running benchmark for: ${TIER_ARR[*]} ==="
python -m genie_bench.runner.run_benchmark --tiers "${TIER_ARR[@]}" --catalog "$CATALOG" --schema "$SCHEMA"
echo "Next: evaluate.py → contamination_check.py → attribute_costs.py → build_results_tables.py"
