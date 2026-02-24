{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month, token)',
    tags=['production','execution','gpay']
  )
}}

SELECT
    toStartOfMonth(date)   AS month,
    symbol                 AS token,
    sum(amount)            AS volume,
    sum(amount_usd)        AS volume_usd,
    sum(payment_count)     AS payments
FROM {{ ref('int_execution_gpay_payments_daily') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY month, token
ORDER BY month, token
