{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, label)',
    tags=['production','execution','gpay']
  )
}}

WITH wd AS (
    SELECT max(date) AS max_date
    FROM {{ ref('int_execution_gpay_payments_daily') }}
),

bounds AS (
    SELECT
        max_date,
        subtractDays(max_date, 7)   AS curr_start,
        max_date                    AS curr_end,
        subtractDays(max_date, 14)  AS prev_start,
        subtractDays(max_date, 7)   AS prev_end
    FROM wd
),

curr_7d AS (
    SELECT
        sum(amount_usd)           AS volume,
        sum(payment_count)        AS payments,
        uniqExact(wallet_address) AS active_users
    FROM {{ ref('int_execution_gpay_payments_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.curr_start AND d.date <= b.curr_end
),

prev_7d AS (
    SELECT
        sum(amount_usd)           AS volume,
        sum(payment_count)        AS payments,
        uniqExact(wallet_address) AS active_users
    FROM {{ ref('int_execution_gpay_payments_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.prev_start AND d.date <= b.prev_end
),

all_time AS (
    SELECT
        sum(amount_usd)           AS volume,
        sum(payment_count)        AS payments,
        uniqExact(wallet_address) AS funded_wallets
    FROM {{ ref('int_execution_gpay_payments_daily') }}
)

SELECT 'Volume' AS label, 'All' AS window,
    round(toFloat64(a.volume), 2) AS value,
    toNullable(NULL) AS change_pct
FROM all_time a

UNION ALL
SELECT 'Volume', '7D',
    round(toFloat64(c.volume), 2),
    round((coalesce(toFloat64(c.volume) / nullIf(toFloat64(p.volume), 0), 0) - 1) * 100, 1)
FROM curr_7d c, prev_7d p

UNION ALL
SELECT 'Payments', 'All',
    toFloat64(a.payments),
    toNullable(NULL)
FROM all_time a

UNION ALL
SELECT 'Payments', '7D',
    toFloat64(c.payments),
    round((coalesce(toFloat64(c.payments) / nullIf(toFloat64(p.payments), 0), 0) - 1) * 100, 1)
FROM curr_7d c, prev_7d p

UNION ALL
SELECT 'ActiveUsers', '7D',
    toFloat64(c.active_users),
    round((coalesce(toFloat64(c.active_users) / nullIf(toFloat64(p.active_users), 0), 0) - 1) * 100, 1)
FROM curr_7d c, prev_7d p

UNION ALL
SELECT 'FundedWallets', 'All',
    toFloat64(a.funded_wallets),
    toNullable(NULL)
FROM all_time a
