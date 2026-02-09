{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:holders_per_token', 'granularity:latest']
  )
}}

SELECT
  symbol      AS token,
  toUInt64(argMax(holders, date)) AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token

