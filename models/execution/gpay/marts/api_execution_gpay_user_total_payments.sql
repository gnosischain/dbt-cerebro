{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_total_payments','granularity:all_time']
  )
}}

SELECT
    wallet_address,
    sum(activity_count) AS value
FROM {{ ref('int_execution_gpay_activity_daily') }}
WHERE action = 'Payment'
GROUP BY wallet_address
