{{
  config(
    materialized='view',
    tags=['dev','execution','tier0','api:active_senders_per_token', 'granularity:daily']
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