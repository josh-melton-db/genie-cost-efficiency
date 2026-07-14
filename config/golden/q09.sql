-- Q9: Edge — nonexistent product code should yield empty/zero
SELECT
  ROUND(COALESCE(SUM(ol.net_recognized_amount), 0), 2) AS recognized_revenue
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_product p ON ol.product_key = p.product_key
WHERE p.sku_code = 'ZX-NOEXIST-999'
  AND ol.line_status = 'FULFILLED';
