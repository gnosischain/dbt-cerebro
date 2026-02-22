{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week)',
    tags=['production','execution','gpay']
  )
}}

WITH weekly_activity AS (
    SELECT
        toStartOfWeek(date, 1)    AS week,
        uniqExact(wallet_address) AS active_users,
        sum(payment_count)        AS total_payments,
        sum(amount_usd)           AS total_volume_usd
    FROM {{ ref('int_execution_gpay_payments_daily') }}
    WHERE toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
),

first_payment AS (
    SELECT
        wallet_address,
        min(date) AS first_date
    FROM {{ ref('int_execution_gpay_payments_daily') }}
    GROUP BY wallet_address
),

weekly_funded AS (
    SELECT
        toStartOfWeek(first_date, 1) AS week,
        count()                      AS newly_funded
    FROM first_payment
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
)

SELECT
    a.week,
    a.active_users,
    a.total_payments,
    round(toFloat64(a.total_volume_usd), 2) AS total_volume_usd,
    coalesce(f.newly_funded, 0)                             AS newly_funded,
    sum(coalesce(f.newly_funded, 0)) OVER (ORDER BY a.week) AS cumulative_funded
FROM weekly_activity a
LEFT JOIN weekly_funded f ON f.week = a.week
ORDER BY a.week
