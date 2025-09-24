{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','gas']
  )
}}

SELECT
  day,
  gas_used_sum / NULLIF(gas_limit_sum, 0) AS used
FROM {{ ref('int_execution_blocks_gas_usage_daily') }}
ORDER BY day DESC