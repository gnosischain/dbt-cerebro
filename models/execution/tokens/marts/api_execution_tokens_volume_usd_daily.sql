{{
  config(
    materialized='view',
    tags=['production','execution','tokens','volume_usd','api']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  volume_usd  AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
ORDER BY
  date,
  token