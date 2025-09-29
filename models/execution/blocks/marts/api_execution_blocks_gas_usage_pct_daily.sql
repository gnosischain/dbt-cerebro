{{
  config(
    materialized='view', 
    tags=['production','execution','transactions','gas']
    )
}}

SELECT
  date,
  gas_used_fraq AS value
FROM {{ ref('int_execution_blocks_gas_usage_daily') }}
WHERE date < today()
ORDER BY date DESC