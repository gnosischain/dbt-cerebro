{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:holders_per_token', 'granularity:latest']
  )
}}

SELECT
  symbol      AS token,
  toUInt64(argMax(holders, date)) AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token

