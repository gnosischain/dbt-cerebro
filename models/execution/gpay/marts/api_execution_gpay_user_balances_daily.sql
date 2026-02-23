{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_balances_daily','granularity:daily']
  )
}}

SELECT
    address AS wallet_address,
    date,
    symbol AS label,
    round(toFloat64(balance_usd), 2) AS value
FROM {{ ref('int_execution_gpay_balances_daily') }}
ORDER BY date
