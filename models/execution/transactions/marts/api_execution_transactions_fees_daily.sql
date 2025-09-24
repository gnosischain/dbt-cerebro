{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
  )
}}
SELECT
  toDate(date)        AS day,
  SUM(fee_native_sum) AS total_fee_native,
  SUM(fee_usd_sum)    AS total_fee_usd
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
GROUP BY day
ORDER BY day DESC