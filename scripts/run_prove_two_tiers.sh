#!/usr/bin/env bash
# End-to-end local driver for the prove-two-tiers milestone.
# Requires: authenticated Databricks CLI profile + CATALOG/SCHEMA env vars.
set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE to your CLI profile}"
export CATALOG="${CATALOG:-genie_tco}"
export SCHEMA="${SCHEMA:-bench}"
export SCALE_PROFILE="${SCALE_PROFILE:-dev}"
TIERS="${TIERS:-t0 t4}"

echo "Profile=$PROFILE catalog=$CATALOG schema=$SCHEMA scale=$SCALE_PROFILE tiers=$TIERS"

python scripts/bootstrap_uc.py --catalog "$CATALOG" --schema "$SCHEMA"
python -m genie_bench.data_gen.generate_raw
# Pipeline + metric views + provision typically via bundle:
#   databricks bundle deploy -t dev --profile "$PROFILE"
#   databricks bundle run pulseforge_medallion -t dev --profile "$PROFILE"
python -m genie_bench.metric_views.build_metric_views --catalog "$CATALOG" --schema "$SCHEMA"
python -m genie_bench.spaces.provision_spaces --tiers $TIERS --catalog "$CATALOG" --schema "$SCHEMA"
RUN_ID=$(python -m genie_bench.runner.run_benchmark --tiers $TIERS --catalog "$CATALOG" --schema "$SCHEMA" | tee /tmp/genie_tco_run.log | awk '/Run complete:/{print $3}')
python -m genie_bench.eval.evaluate --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
python scripts/contamination_check.py --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
echo "Waiting for billing settle is recommended before cost attribution."
python -m genie_bench.cost.attribute_costs --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
python -m genie_bench.report.build_results_tables --run-id "$RUN_ID" --catalog "$CATALOG" --schema "$SCHEMA"
echo "Done. Inspect $CATALOG.$SCHEMA.metric_tco for \$/correct."
