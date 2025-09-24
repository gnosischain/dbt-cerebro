{{
  config(
    materialized='view',
    tags=['production','execution','transactions']
  )
}}

SELECT
  date_trunc('month', toDate(date)) AS month,
  SUM(n_txs)                        AS total
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE date > now() - INTERVAL 1095 DAY
  AND success = 1
GROUP BY month
ORDER BY month DESC