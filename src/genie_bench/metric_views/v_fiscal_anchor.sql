-- One-row fiscal anchor relative to current_date() on the PulseForge 4-4-5 calendar.
-- Joined into metric views so Genie can filter `Is Last Fiscal Month` etc. instead of
-- inventing calendar YEAR()/MONTH() against fiscal labels.
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_fiscal_anchor
COMMENT 'PulseForge fiscal period anchors relative to current_date() (4-4-5). Tag: certified.'
AS
WITH today AS (
  SELECT fiscal_year, fiscal_month, fiscal_quarter
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = current_date()
  LIMIT 1
),
last_fm AS (
  SELECT fiscal_year, fiscal_month, fiscal_quarter
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = add_months(current_date(), -1)
  LIMIT 1
),
last_fq AS (
  SELECT fiscal_year, fiscal_quarter
  FROM ${catalog}.${schema}.dim_date
  WHERE calendar_date = add_months(current_date(), -3)
  LIMIT 1
)
SELECT
  1 AS anchor_key,
  t.fiscal_year AS current_fy,
  t.fiscal_month AS current_fm,
  t.fiscal_quarter AS current_fq,
  lf.fiscal_year AS last_fm_year,
  lf.fiscal_month AS last_fm_month,
  lf.fiscal_quarter AS last_fm_quarter,
  lq.fiscal_year AS last_fq_year,
  lq.fiscal_quarter AS last_fq
FROM today t
CROSS JOIN last_fm lf
CROSS JOIN last_fq lq;
