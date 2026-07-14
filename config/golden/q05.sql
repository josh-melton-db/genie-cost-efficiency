-- Q5: Active members in EMEA this fiscal year
-- Active = proprietary: has ACTIVATE/RENEW in last 90 days AND no CANCEL after last activate
WITH fy AS (
  SELECT fiscal_year
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = current_date()
  LIMIT 1
),
last_events AS (
  SELECT
    se.member_key,
    MAX(CASE WHEN se.event_type IN ('ACTIVATE', 'RENEW') THEN d.calendar_date END) AS last_active_dt,
    MAX(CASE WHEN se.event_type = 'CANCEL' THEN d.calendar_date END) AS last_cancel_dt
  FROM ${catalog}.${schema}.fact_subscription_event se
  JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
  JOIN fy ON d.fiscal_year = fy.fiscal_year
  GROUP BY se.member_key
)
SELECT COUNT(*) AS active_members_emea
FROM last_events le
JOIN ${catalog}.${schema}.dim_member m ON le.member_key = m.member_key
JOIN ${catalog}.${schema}.dim_geo g ON m.home_geo_key = g.geo_key
WHERE g.region_code = 'EMEA'
  AND le.last_active_dt >= date_sub(current_date(), 90)
  AND (le.last_cancel_dt IS NULL OR le.last_cancel_dt < le.last_active_dt);
