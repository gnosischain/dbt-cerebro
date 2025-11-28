{{
  config(
    materialized='view',
    tags=['production','execution','blocks','gas', 'tier1', 'api: gas_usage_pct_m']
  )
}}

SELECT
  month AS date,
  ROUND(used * 100, 2) AS value
FROM {{ ref('fct_execution_blocks_gas_usage_monthly') }}
ORDER BY date