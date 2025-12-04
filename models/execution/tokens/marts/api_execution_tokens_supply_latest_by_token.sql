{{
  config(
    materialized='view',
    tags=['production','execution','tokens','supply_latest','api']
  )
}}

SELECT
  symbol      AS token,
  argMax(supply, date) AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
GROUP BY token_address, symbol
ORDER BY token

