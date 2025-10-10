{{
  config(
    materialized='view',
    tags=['production','execution','transactions']
  )
}}

WITH m AS (
  SELECT
    toStartOfMonth(date)            AS month,
    project,
    sum(tx_count)                   AS txs
  FROM {{ ref('int_execution_transactions_by_project_daily') }}
  WHERE date < toStartOfMonth(today())       
  GROUP BY month, project
),

r AS (
  SELECT
    month,
    project,
    txs,
    row_number() OVER (PARTITION BY month ORDER BY txs DESC) AS rn
  FROM m
)

SELECT
  month                         AS date,
  if(rn <= 5, project, 'Others') AS label,
  sum(txs)                      AS value
FROM r
GROUP BY date, label
ORDER BY date DESC, value DESC