{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  date_trunc('month', date) AS month,
  SUM(n_txs)                AS total
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY month
ORDER BY month DESC