{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}

SELECT
  date,
  ROUND(SUM(fee_native_sum), 6) AS value
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
  AND date < today()   
GROUP BY date
ORDER BY date DESC