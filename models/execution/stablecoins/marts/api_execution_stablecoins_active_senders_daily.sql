{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_active_senders', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol AS token,
  token_class,
  active_senders AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
  AND token_class = 'STABLECOIN'
ORDER BY
  date,
  token

