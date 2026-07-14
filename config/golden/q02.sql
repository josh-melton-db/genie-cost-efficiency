-- Q2: Recognized revenue by geo region for last fiscal quarter
WITH last_fq AS (
  SELECT fiscal_year, fiscal_quarter
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = add_months(current_date(), -3)
  LIMIT 1
)
SELECT
  g.region_name,
  ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
JOIN ${catalog}.${schema}.dim_geo g ON ol.geo_key = g.geo_key
JOIN last_fq fq
  ON d.fiscal_year = fq.fiscal_year
 AND d.fiscal_quarter = fq.fiscal_quarter
WHERE ol.line_status = 'FULFILLED'
GROUP BY g.region_name
ORDER BY recognized_revenue DESC;
