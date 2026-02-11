{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, token)',
    tags=['production','execution','gpay']
  )
}}

SELECT
    toStartOfWeek(date, 1) AS week,
    symbol                 AS token,
    sum(amount)            AS volume,
    sum(amount_usd)        AS volume_usd,
    sum(payment_count)     AS payments
FROM {{ ref('int_execution_gpay_payments_daily') }}
WHERE toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
GROUP BY week, token
ORDER BY week, token
