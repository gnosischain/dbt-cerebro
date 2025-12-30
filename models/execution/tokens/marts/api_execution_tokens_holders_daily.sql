{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:holders_per_token', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  holders     AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
ORDER BY
  date,
  token