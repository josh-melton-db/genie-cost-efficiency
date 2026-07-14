-- Q4: "How is the business doing?" — curated KPI pack
-- LLM-judge scores whether Genie returns this shape or asks clarifying questions
WITH fytd AS (
  SELECT fiscal_year
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = current_date()
  LIMIT 1
),
rev AS (
  SELECT ROUND(SUM(ol.net_recognized_amount), 2) AS recognized_revenue_fytd
  FROM ${catalog}.${schema}.fact_order_line ol
  JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
  JOIN fytd ON d.fiscal_year = fytd.fiscal_year
  WHERE ol.line_status = 'FULFILLED'
),
mrr AS (
  SELECT ROUND(SUM(CASE WHEN se.event_type IN ('ACTIVATE', 'RENEW') THEN se.mrr_delta
                        WHEN se.event_type = 'CANCEL' THEN se.mrr_delta ELSE 0 END), 2) AS ending_mrr
  FROM ${catalog}.${schema}.fact_subscription_event se
  JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
  JOIN fytd ON d.fiscal_year = fytd.fiscal_year
),
churn AS (
  SELECT
    COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END)
      / NULLIF(COUNT(DISTINCT se.member_key), 0) AS churn_rate_fytd
  FROM ${catalog}.${schema}.fact_subscription_event se
  JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
  JOIN fytd ON d.fiscal_year = fytd.fiscal_year
)
SELECT rev.recognized_revenue_fytd, mrr.ending_mrr, ROUND(churn.churn_rate_fytd, 4) AS churn_rate_fytd
FROM rev CROSS JOIN mrr CROSS JOIN churn;
