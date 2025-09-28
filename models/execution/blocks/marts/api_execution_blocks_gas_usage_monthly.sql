{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','gas']
    )
}}

WITH monthly AS (
  SELECT
    date_trunc('month', date) AS date,
    SUM(gas_used_sum)         AS gas_used_sum_monthly,
    SUM(gas_limit_sum)        AS gas_limit_sum_monthly
  FROM {{ ref('int_execution_blocks_gas_usage_daily') }}
  WHERE date < date_trunc('month', today())   
  GROUP BY date
)
SELECT
  date,
  gas_used_sum_monthly / NULLIF(gas_limit_sum_monthly, 0) AS value
FROM monthly
ORDER BY date DESC