{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_cashback_daily','granularity:daily']
  )
}}

SELECT
    wallet_address,
    date,
    round(toFloat64(amount), 6) AS value
FROM {{ ref('int_execution_gpay_cashback_daily') }}
ORDER BY date
