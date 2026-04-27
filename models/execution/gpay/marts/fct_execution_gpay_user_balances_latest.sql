{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(wallet_address, token)',
    unique_key='(wallet_address, token)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production', 'execution', 'gpay', 'user_portfolio', 'balances', 'granularity:latest']
  )
}}

WITH latest_date AS (
  SELECT max(date) AS date
  FROM {{ ref('int_execution_gpay_balances_daily') }}
  WHERE date < today()
)

SELECT
  address AS wallet_address,
  symbol AS token,
  sum(round(toFloat64(balance_usd), 2)) AS value_usd,
  sum(round(toFloat64(balance), 6)) AS value_native,
  max(date) AS date
FROM {{ ref('int_execution_gpay_balances_daily') }}
WHERE date = (SELECT date FROM latest_date)
GROUP BY wallet_address, token
