-- Helper views for mv_pulseforge_conformed_wide (one-to-many joins require UC table sources).

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_wide_members
COMMENT 'Member spine with exact active-member flag for wide conformed MV'
AS
WITH fy AS (
  SELECT current_fy AS fiscal_year FROM ${catalog}.${schema}.v_fiscal_anchor
),
active AS (
  SELECT
    se.member_key,
    MAX(CASE WHEN se.event_type IN ('ACTIVATE', 'RENEW') THEN d.calendar_date END) AS last_active_dt,
    MAX(CASE WHEN se.event_type = 'CANCEL' THEN d.calendar_date END) AS last_cancel_dt
  FROM ${catalog}.${schema}.fact_subscription_event se
  JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
  JOIN fy ON d.fiscal_year = fy.fiscal_year
  GROUP BY se.member_key
)
SELECT
  m.*,
  g.region_name,
  g.region_code,
  CASE
    WHEN a.last_active_dt >= date_sub(current_date(), 90)
     AND (a.last_cancel_dt IS NULL OR a.last_cancel_dt < a.last_active_dt)
    THEN TRUE ELSE FALSE
  END AS is_active_member,
  CASE
    WHEN a.last_active_dt >= date_sub(current_date(), 90)
     AND (a.last_cancel_dt IS NULL OR a.last_cancel_dt < a.last_active_dt)
    THEN 1 ELSE 0
  END AS active_member_flag
FROM ${catalog}.${schema}.dim_member m
LEFT JOIN ${catalog}.${schema}.dim_geo g ON m.home_geo_key = g.geo_key
LEFT JOIN active a ON m.member_key = a.member_key;

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_wide_orders
COMMENT 'Fulfilled order lines with fiscal/hardware amount buckets for wide conformed MV'
AS
SELECT
  ol.member_key,
  ol.order_line_key,
  ol.order_key,
  m.member_id,
  m.display_handle,
  ol.net_recognized_amount,
  CASE WHEN p.product_class IN ('DEVICE', 'ACCESSORY') THEN ol.net_recognized_amount ELSE 0 END AS hardware_amount,
  CASE WHEN p.product_class IN ('DEVICE', 'ACCESSORY')
        AND d.fiscal_year = a.current_fy AND d.fiscal_month <= a.current_fm
       THEN ol.net_recognized_amount ELSE 0 END AS hardware_fytd_amount,
  CASE WHEN d.fiscal_year = a.last_fm_year AND d.fiscal_month = a.last_fm_month
       THEN ol.net_recognized_amount ELSE 0 END AS last_fm_amount,
  CASE WHEN d.fiscal_year = a.last_fq_year AND d.fiscal_quarter = a.last_fq
       THEN ol.net_recognized_amount ELSE 0 END AS last_fq_amount,
  CASE WHEN d.fiscal_year = a.current_fy AND d.fiscal_month <= a.current_fm
       THEN ol.net_recognized_amount ELSE 0 END AS fytd_amount,
  CASE WHEN d.fiscal_year = a.current_fy AND d.fiscal_quarter = 1
       THEN ol.net_recognized_amount ELSE 0 END AS q1_amount,
  CASE WHEN d.fiscal_year = a.current_fy AND d.fiscal_quarter = 2
       THEN ol.net_recognized_amount ELSE 0 END AS q2_amount,
  CASE WHEN d.fiscal_year = 2025 AND d.fiscal_month = 6
       THEN ol.net_recognized_amount ELSE 0 END AS june_spike_amount,
  d.fiscal_year,
  d.fiscal_month,
  d.fiscal_quarter,
  g.region_name AS order_region_name,
  g.region_code AS order_region_code,
  p.product_family,
  p.product_class,
  p.sku_code,
  c.campaign_code,
  c.campaign_name
FROM ${catalog}.${schema}.fact_order_line ol
JOIN ${catalog}.${schema}.dim_date d ON ol.recognition_date_key = d.date_key
JOIN ${catalog}.${schema}.dim_geo g ON ol.geo_key = g.geo_key
JOIN ${catalog}.${schema}.dim_product p ON ol.product_key = p.product_key
JOIN ${catalog}.${schema}.dim_campaign c ON ol.campaign_key = c.campaign_key
CROSS JOIN ${catalog}.${schema}.v_fiscal_anchor a
JOIN ${catalog}.${schema}.dim_member m ON ol.member_key = m.member_key
WHERE ol.line_status = 'FULFILLED';

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_wide_subs
COMMENT 'Subscription events with retention/FYTD buckets for wide conformed MV'
AS
SELECT
  se.member_key,
  se.subscription_event_key,
  se.event_type,
  se.mrr_delta,
  CASE WHEN d.fiscal_year = a.current_fy AND d.fiscal_month <= a.current_fm
       THEN se.mrr_delta ELSE 0 END AS fytd_mrr_delta,
  CASE WHEN se.event_type = 'CANCEL' THEN 1 ELSE 0 END AS is_cancel,
  CASE WHEN d.calendar_date BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
       THEN 1 ELSE 0 END AS in_retention_window,
  CASE WHEN se.event_type = 'CANCEL'
        AND d.calendar_date BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
       THEN 1 ELSE 0 END AS retention_cancel,
  d.fiscal_year,
  d.fiscal_month,
  c.campaign_code,
  c.campaign_name
FROM ${catalog}.${schema}.fact_subscription_event se
JOIN ${catalog}.${schema}.dim_date d ON se.event_date_key = d.date_key
JOIN ${catalog}.${schema}.dim_campaign c ON se.attribution_campaign_key = c.campaign_key
CROSS JOIN ${catalog}.${schema}.v_fiscal_anchor a;
