{{ config(materialized='view', tags=['production','execution','transactions']) }}

WITH totals AS (
  SELECT
    date,
    active_accounts
  FROM {{ ref('fct_execution_transactions_active_accounts_daily') }}
  WHERE date < today()
),
latest_date AS (
  SELECT date
  FROM totals
  ORDER BY date DESC
  LIMIT 1
),
curr AS (
  SELECT t.date, t.active_accounts
  FROM totals t
  JOIN latest_date d ON t.date = d.date
),
prev7 AS (
  SELECT t.date, t.active_accounts
  FROM totals t
  JOIN latest_date d ON t.date = subtractDays(d.date, 7)
)
SELECT
  c.date,
  c.active_accounts AS value,
  ROUND((COALESCE(c.active_accounts / NULLIF(p.active_accounts, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr c
CROSS JOIN prev7 p