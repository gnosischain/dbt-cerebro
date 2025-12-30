{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:tokens_supply', 'granularity:latest']
  )
}}

SELECT
  symbol      AS token,
  argMax(supply, date) AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token

