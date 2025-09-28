{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
    )
}}

SELECT
  date,
  SUM(fee_usd_sum) AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
  AND date < today()
GROUP BY date
ORDER BY date DESC