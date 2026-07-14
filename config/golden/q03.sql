-- Q3: Top 10 members by lifetime recognized hardware revenue fiscal YTD
WITH fytd AS (
  SELECT fiscal_year
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = current_date()
  LIMIT 1
)
SELECT
  m.member_id,
  m.display_handle,
  ROUND(SUM(ol.net_recognized_amount), 2) AS hardware_revenue_fytd
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_product p ON ol.product_key = p.product_key
JOIN ${catalog}.${schema}.dim_member m ON ol.member_key = m.member_key
JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
JOIN fytd ON d.fiscal_year = fytd.fiscal_year
WHERE ol.line_status = 'FULFILLED'
  AND p.product_class IN ('DEVICE', 'ACCESSORY')
GROUP BY m.member_id, m.display_handle
ORDER BY hardware_revenue_fytd DESC
LIMIT 10;
