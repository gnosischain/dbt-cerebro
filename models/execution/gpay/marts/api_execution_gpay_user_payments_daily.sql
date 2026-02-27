{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_payments_daily','granularity:daily']
  )
}}

SELECT
    wallet_address,
    date,
    symbol AS label,
    round(toFloat64(amount_usd), 2) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Payment'
ORDER BY date
