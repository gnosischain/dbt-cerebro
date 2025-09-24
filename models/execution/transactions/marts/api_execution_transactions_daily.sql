{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  toDate(date) AS day,
  SUM(n_txs)   AS total
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY day
ORDER BY day DESC