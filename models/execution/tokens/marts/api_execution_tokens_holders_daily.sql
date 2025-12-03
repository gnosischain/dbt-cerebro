{{
  config(
    materialized='view',
    tags=['production','execution','tokens','holders','api']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  holders     AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
ORDER BY
  date,
  token