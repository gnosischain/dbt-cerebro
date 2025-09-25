{{
  config(
    materialized='view', 
    tags=['production','execution','blocks','gas']
  )
}}

SELECT
  month AS date,
  ROUND(used * 100,2) AS value
FROM {{ ref('fct_execution_blocks_gas_usage_monthly') }}
ORDER BY month