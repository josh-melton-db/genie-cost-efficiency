# Genie `serialized_space` v2 wire samples

Ground-truth fixtures for the Genie TCO compiler.

| File | Source |
|------|--------|
| `t4_live_export.json` | Live export of prove-out T4 space (`GET ...?include_serialized_space=true`) |
| `canonical_v2_full.json` | Full v2 schema exercising every construct we automate (docs + schema.md) |

## Field names that matter for the sweep

- **Prompt matching (v2):** `enable_format_assistance`, `enable_entity_matching` (not v1 `get_example_values` / `build_value_dictionary`)
- **Hide columns:** `column_configs[].exclude: true`
- **Joins:** `sql` must be `[condition, "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]` (or ONE_TO_MANY / ONE_TO_ONE / MANY_TO_MANY); `left`/`right` are `{identifier, alias}` objects
- **SQL expressions:** `sql_snippets.{filters,expressions,measures}` (expressions = fields/dimensions)
- **Trusted assets:** parameterized `example_question_sqls` (`parameters[]`) and `sql_functions[]` UC UDFs
- **Sorting:** tables by `identifier`; column_configs by `column_name`; all ID collections alphabetically by `id`

## Limits (compiler warns)

- ≤ 30 tables/metric views per agent
- ≤ 100 instructions (each example SQL + each SQL function + the text block = 1)
- ≤ 200 knowledge-store snippets (table descriptions + joins + SQL expressions)
