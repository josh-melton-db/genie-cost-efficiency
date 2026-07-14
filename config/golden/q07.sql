-- Q7: Why did revenue spike in June? — attribute to PulseForge X1 launch
-- LLM-judge checks for product family + campaign attribution
SELECT
  p.product_family,
  p.sku_code,
  c.campaign_code,
  c.campaign_name,
  d.fiscal_year,
  d.fiscal_month,
  ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue,
  COUNT(DISTINCT ol.order_key) AS order_count
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_product p ON ol.product_key = p.product_key
JOIN ${catalog}.${schema}.dim_campaign c ON ol.campaign_key = c.campaign_key
JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
WHERE d.fiscal_month = 6
  AND d.fiscal_year = 2025
  AND ol.line_status = 'FULFILLED'
GROUP BY ALL
ORDER BY recognized_revenue DESC
LIMIT 20;
