{{ 
  config(
    materialized='view',
    tags=['production','execution','transactions']
  ) 
}}

SELECT
  toStartOfWeek(date)        AS date,          
  transaction_type           AS label,
  sum(gas_used)              AS value
  -- avg(gas_price_avg)         AS gas_price_avg,     
  -- median(gas_price_median)   AS gas_price_median   
FROM {{ ref('int_execution_transactions_info_daily') }}
WHERE success = 1
  AND date < toStartOfWeek(today())               
GROUP BY date, label
ORDER BY date, label