{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_volume', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  volume_token AS value_native,
  volume_usd   AS value_usd
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
ORDER BY
  date,
  token
