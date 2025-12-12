{{
  config(
    materialized='view',
    tags=['production','execution', 'tier1', 'api:blocks_gas_usage_pct', 'granularity:monthly']
  )
}}

SELECT
  month AS date,
  ROUND(used * 100, 2) AS value
FROM {{ ref('fct_execution_blocks_gas_usage_monthly') }}
ORDER BY date