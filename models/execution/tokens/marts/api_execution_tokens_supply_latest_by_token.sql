{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_supply', 'granularity:latest']
  )
}}

SELECT
  symbol      AS token,
  argMax(supply, date) AS value_native,
  argMax(supply_usd, date) AS value_usd
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
GROUP BY symbol
ORDER BY token

