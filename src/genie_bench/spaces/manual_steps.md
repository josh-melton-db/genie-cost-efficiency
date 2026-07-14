# Residual UI / REST steps not fully covered by serialized_space v2

Most curation axes are now automated via `compile_space.py` + `provision_spaces.py`
(grounded against `wire_samples/canonical_v2_full.json` and live exports).

These steps are also listed per-tier in `config/tiers/*.yaml` (`manual_steps`) and
echoed into `provisioned_state.json` after provisioning.

## Cross-cutting

1. Confirm each tier warehouse is **Serverless** and **Medium** (or configured size).
2. Confirm each tier SP has `SELECT`/`EXECUTE` on the benchmark schema and `CAN USE` on its warehouse
   (attempted automatically in `provision_spaces.py`).
3. SP OAuth secrets are created and stored in scope `genie-tco-bench` as
   `sp-tN-client-id` / `sp-tN-client-secret` during provision.
4. Each SP is granted **Can Run** on its Genie space (attempted automatically).
5. Run `python -m genie_bench.spaces.build_sql_functions` before provisioning T17/T4/T21.

## Prompt matching

API defaults prompt matching **OFF**. Tiers T4/T11/T21 set
`enable_format_assistance` / `enable_entity_matching` in `column_configs`.
After first provision, spot-check Configure → Data → column → Advanced in the UI.

## T2 / T3 / T19

- Tag metric views `certified = true` (also attempted in `build_metric_views.py`).

## T5 / T23

- Confirm distractor gold tables exist (created by the SDP pipeline / materialize_gold).

## Agent mode

Out of scope for this benchmark (standard mode / Conversation API only).
