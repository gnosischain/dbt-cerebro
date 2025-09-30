{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH totals AS (
  SELECT
    date,
    SUM(fee_usd_sum) AS total_fee_usd
  FROM {{ ref('int_execution_transactions_info_daily') }}
  WHERE success = 1
    AND date < today()
  GROUP BY date
),
latest_date AS (
  SELECT date
  FROM totals
  ORDER BY date DESC
  LIMIT 1
),
curr AS (
  SELECT t.date, t.total_fee_usd
  FROM totals t
  JOIN latest_date d ON t.date = d.date
),
prev7 AS (
  SELECT t.date, t.total_fee_usd
  FROM totals t
  JOIN latest_date d ON t.date = subtractDays(d.date, 7)
)
SELECT
  c.date,
  c.total_fee_usd AS value,
  ROUND((COALESCE(c.total_fee_usd / NULLIF(p.total_fee_usd, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr c
CROSS JOIN prev7 p