{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, token)',
    tags=['production','execution','gpay']
  )
}}

SELECT
    date,
    symbol                 AS token,
    sum(amount)            AS volume,
    sum(amount_usd)        AS volume_usd,
    sum(payment_count)     AS payments
FROM {{ ref('int_execution_gpay_payments_daily') }}
WHERE date < today()
GROUP BY date, token
ORDER BY date, token
