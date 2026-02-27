{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_total_cashback','granularity:all_time']
  )
}}

SELECT
    wallet_address,
    round(toFloat64(sum(amount)), 6) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Cashback'
GROUP BY wallet_address
