-- Q6: Fiscal Q1 vs Q2 recognized revenue by product family
WITH fy AS (
  SELECT fiscal_year
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = current_date()
  LIMIT 1
)
SELECT
  p.product_family,
  ROUND(SUM(CASE WHEN d.fiscal_quarter = 1 THEN ol.net_recognized_amount ELSE 0 END), 2) AS q1_revenue,
  ROUND(SUM(CASE WHEN d.fiscal_quarter = 2 THEN ol.net_recognized_amount ELSE 0 END), 2) AS q2_revenue
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_product p ON ol.product_key = p.product_key
JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
JOIN fy ON d.fiscal_year = fy.fiscal_year
WHERE ol.line_status = 'FULFILLED'
  AND d.fiscal_quarter IN (1, 2)
GROUP BY p.product_family
ORDER BY p.product_family;
