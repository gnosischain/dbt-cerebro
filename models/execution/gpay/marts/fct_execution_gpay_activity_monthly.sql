{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month)',
    tags=['production','execution','gpay']
  )
}}

WITH monthly_activity AS (
    SELECT
        toStartOfMonth(date)      AS month,
        uniqExact(wallet_address) AS active_users,
        sum(payment_count)        AS total_payments,
        sum(amount_usd)           AS total_volume_usd
    FROM {{ ref('int_execution_gpay_payments_daily') }}
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY month
),

first_payment AS (
    SELECT
        wallet_address,
        min(date) AS first_date
    FROM {{ ref('int_execution_gpay_payments_daily') }}
    GROUP BY wallet_address
),

monthly_funded AS (
    SELECT
        toStartOfMonth(first_date) AS month,
        count()                    AS newly_funded
    FROM first_payment
    WHERE toStartOfMonth(first_date) < toStartOfMonth(today())
    GROUP BY month
)

SELECT
    a.month,
    a.active_users,
    a.total_payments,
    round(toFloat64(a.total_volume_usd), 2) AS total_volume_usd,
    coalesce(f.newly_funded, 0)                               AS newly_funded,
    sum(coalesce(f.newly_funded, 0)) OVER (ORDER BY a.month)  AS cumulative_funded
FROM monthly_activity a
LEFT JOIN monthly_funded f ON f.month = a.month
ORDER BY a.month
