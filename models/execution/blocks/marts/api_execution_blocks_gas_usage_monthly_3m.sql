{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','gas']
  )
}}

SELECT
  month,
  used
FROM {{ ref('fct_execution_blocks_gas_usage_monthly') }}
WHERE month > now() - INTERVAL 3 MONTH
ORDER BY month DESC