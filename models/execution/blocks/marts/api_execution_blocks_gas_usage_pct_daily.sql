{{
  config(
    materialized='view',
    tags=['production','execution','blocks','gas', 'tier1', 'api: gas_usage_pct_d']
  )
}}

SELECT
  date,
  ROUND(gas_used_fraq * 100, 2) AS value
FROM {{ ref('int_execution_blocks_gas_usage_daily') }}
WHERE date < today()   
ORDER BY date