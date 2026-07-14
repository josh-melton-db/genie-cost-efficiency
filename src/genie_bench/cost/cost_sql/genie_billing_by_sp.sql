-- Genie LLM billables by service principal (tier run_as)
-- Docs: https://docs.databricks.com/aws/en/genie/budgets
-- Genie usage is hour-bucketed (usage_start_time is the hour floor), so filter by
-- bucket overlap with the run window rather than strict containment.
-- Params: {{start_ts}}, {{end_ts}}, {{sp_list}}
SELECT
  u.usage_date,
  u.identity_metadata.run_as AS run_as_sp,
  u.usage_metadata.genie.surface AS genie_surface,
  u.sku_name,
  SUM(u.usage_quantity) AS billable_dbus,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS list_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.cloud = lp.cloud
 AND u.sku_name = lp.sku_name
 AND u.usage_start_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.billing_origin_product = 'GENIE'
  -- Hour-bucket overlap: bill hour intersects [start_ts, end_ts)
  AND u.usage_start_time < TIMESTAMP '{{end_ts}}'
  AND COALESCE(u.usage_end_time, u.usage_start_time + INTERVAL 1 HOUR) > TIMESTAMP '{{start_ts}}'
  AND u.identity_metadata.run_as IN ({{sp_list}})
GROUP BY ALL
ORDER BY billable_dbus DESC;
