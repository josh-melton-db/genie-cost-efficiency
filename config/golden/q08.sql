-- Q8: Which campaigns drove subscription churn reduction?
WITH cohort AS (
  SELECT
    c.campaign_code,
    c.campaign_name,
    COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END) AS cancels,
    COUNT(DISTINCT se.member_key) AS touched_members,
    COUNT(DISTINCT CASE WHEN se.event_type = 'CANCEL' THEN se.member_key END)
      / NULLIF(COUNT(DISTINCT se.member_key), 0) AS churn_rate
  FROM ${catalog}.${schema}.fact_subscription_event se
  JOIN ${catalog}.${schema}.dim_campaign c ON se.attribution_campaign_key = c.campaign_key
  JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
  WHERE d.calendar_date BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
  GROUP BY c.campaign_code, c.campaign_name
)
SELECT *
FROM cohort
ORDER BY churn_rate ASC
LIMIT 10;
