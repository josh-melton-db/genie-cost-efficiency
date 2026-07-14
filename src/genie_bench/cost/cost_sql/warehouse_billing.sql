-- Exact warehouse $ for dedicated tier warehouses
-- Serverless SQL compute is billed in hourly buckets whose usage_start_time is the
-- hour floor (e.g. 03:00:00 for work at 03:48). Filter by bucket overlap with the
-- run window, not strict containment of usage_start_time inside [start, end).
SELECT
  u.usage_date,
  u.usage_metadata.warehouse_id AS warehouse_id,
  SUM(u.usage_quantity) AS warehouse_dbus,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS warehouse_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.cloud = lp.cloud
 AND u.sku_name = lp.sku_name
 AND u.usage_start_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.usage_metadata.warehouse_id IN ({{warehouse_id_list}})
  AND u.usage_start_time < TIMESTAMP '{{end_ts}}'
  AND COALESCE(u.usage_end_time, u.usage_start_time + INTERVAL 1 HOUR) > TIMESTAMP '{{start_ts}}'
GROUP BY ALL
ORDER BY warehouse_cost_usd DESC;
