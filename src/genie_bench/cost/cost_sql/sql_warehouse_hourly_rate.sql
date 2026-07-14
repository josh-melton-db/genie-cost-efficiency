-- Effective list $/DBU for serverless SQL SKUs used by the benchmark warehouses.
-- Combined with warehouse size DBU/hour → $/hour for query-time prorating.
-- Params: {{warehouse_id_list}}
SELECT
  AVG(lp.pricing.effective_list.default) AS usd_per_dbu,
  ANY_VALUE(u.sku_name) AS sku_name
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.cloud = lp.cloud
 AND u.sku_name = lp.sku_name
 AND u.usage_start_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE u.usage_metadata.warehouse_id IN ({{warehouse_id_list}})
  AND u.usage_date >= current_date() - INTERVAL 14 DAYS
  AND u.sku_name LIKE '%SERVERLESS_SQL%'
;