{{
  config(
    materialized='view',
    tags=['production','execution','tokens','holders_latest','api']
  )
}}

SELECT
  symbol      AS token,
  toUInt64(argMax(holders, date)) AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token

