{{
  config(
    materialized='view',
    tags=['production','execution','tokens','active_senders','api']
  )
}}

SELECT
  date,
  symbol          AS token,
  token_class,
  active_senders  AS value
FROM {{ ref('int_execution_tokens_value_daily') }}
WHERE date < today()
ORDER BY
  date,
  token