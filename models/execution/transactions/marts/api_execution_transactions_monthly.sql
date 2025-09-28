{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
    )
}}

SELECT
  date_trunc('month', date) AS date,
  SUM(n_txs)                AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
  AND date < date_trunc('month', today())
GROUP BY date
ORDER BY date DESC