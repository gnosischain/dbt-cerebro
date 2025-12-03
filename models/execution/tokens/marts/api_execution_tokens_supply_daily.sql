{{
  config(
    materialized='view',
    tags=['production','execution','tokens','supply_daily','api']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  supply      AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
ORDER BY
  date,
  token