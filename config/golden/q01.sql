-- Q1 / Q10: Total recognized revenue last fiscal month
-- Ambiguous without curation: order_ts vs ship_ts vs recognition_ts; gross vs net-of-returns
WITH last_fm AS (
  SELECT fiscal_year, fiscal_month
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = add_months(current_date(), -1)
  LIMIT 1
)
SELECT ROUND(SUM(ol.net_recognized_amount), 2) AS total_recognized_revenue
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_date d
  ON ol.recognition_date_key = d.date_key
JOIN last_fm lf
  ON d.fiscal_year = lf.fiscal_year
 AND d.fiscal_month = lf.fiscal_month
WHERE ol.line_status = 'FULFILLED';
