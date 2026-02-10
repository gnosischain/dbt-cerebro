{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_volume_payments_by_token_weekly','granularity:weekly']
  )
}}

SELECT
    toStartOfWeek(date, 1) AS week,
    symbol                 AS token,
    sum(amount)            AS volume,
    sum(amount_usd)        AS volume_usd,
    sum(payment_count)     AS payments
FROM {{ ref('int_execution_gpay_payments_daily') }}
WHERE date < today()
GROUP BY week, token
ORDER BY week, token
