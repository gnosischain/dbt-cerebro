{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
)
}}

WITH totals AS (
  SELECT
    date,
    SUM(n_txs) AS total_txs
  FROM {{ ref('int_execution_transactions_info_daily') }}
  WHERE success = 1
  GROUP BY date
),
latest_date AS (
  SELECT max(date) AS date
  FROM totals
  WHERE date < today()       
),
curr AS (
  SELECT t.date, t.total_txs
  FROM totals t
  JOIN latest_date d ON t.date = d.date
),
prev7 AS (
  SELECT t.date, t.total_txs
  FROM totals t
  JOIN latest_date d ON t.date = subtractDays(d.date, 7)
)
SELECT
  c.date,
  c.total_txs AS value,
  ROUND( (COALESCE(c.total_txs / NULLIF(p.total_txs, 0), 0) - 1) * 100, 1) AS change_pct
FROM curr c
CROSS JOIN prev7 p