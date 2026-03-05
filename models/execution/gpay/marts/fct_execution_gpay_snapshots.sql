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
    FROM {{ ref('int_execution_gpay_activity_daily') }}
),

bounds AS (
    SELECT
        max_date,
        subtractDays(max_date, 7)  AS curr_start,
        max_date                   AS curr_end,
        subtractDays(max_date, 14) AS prev_start,
        subtractDays(max_date, 7)  AS prev_end
    FROM wd
),

curr_7d AS (
    SELECT
        action,
        sum(amount_usd)           AS volume,
        sum(amount)               AS native_volume,
        sum(activity_count)       AS cnt,
        uniqExact(wallet_address) AS users
    FROM {{ ref('int_execution_gpay_activity_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.curr_start AND d.date <= b.curr_end
    GROUP BY action
),

prev_7d AS (
    SELECT
        action,
        sum(amount_usd)           AS volume,
        sum(amount)               AS native_volume,
        sum(activity_count)       AS cnt,
        uniqExact(wallet_address) AS users
    FROM {{ ref('int_execution_gpay_activity_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.prev_start AND d.date <= b.prev_end
    GROUP BY action
),

all_time AS (
    SELECT
        action,
        sum(amount_usd)           AS volume,
        sum(amount)               AS native_volume,
        sum(activity_count)       AS cnt,
        uniqExact(wallet_address) AS users
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    GROUP BY action
),

joined AS (
    SELECT
        a.action AS action_type,
        a.volume  AS all_volume,  a.native_volume AS all_native,  a.cnt AS all_cnt,  a.users AS all_users,
        c.volume  AS curr_volume, c.native_volume AS curr_native, c.cnt AS curr_cnt, c.users AS curr_users,
        p.volume  AS prev_volume, p.native_volume AS prev_native, p.cnt AS prev_cnt, p.users AS prev_users
    FROM all_time a
    LEFT JOIN curr_7d c ON c.action = a.action
    LEFT JOIN prev_7d p ON p.action = a.action
),

-- Cashback-specific bounds: use the last distribution week instead of a strict 7D window
cashback_bounds AS (
    SELECT
        toStartOfWeek(max(date), 1) AS curr_week,
        subtractWeeks(toStartOfWeek(max(date), 1), 1) AS prev_week
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Cashback'
),

curr_cb AS (
    SELECT
        sum(amount_usd)           AS volume,
        sum(amount)               AS native_volume,
        sum(activity_count)       AS cnt,
        uniqExact(wallet_address) AS users
    FROM {{ ref('int_execution_gpay_activity_daily') }} d
    CROSS JOIN cashback_bounds cb
    WHERE d.action = 'Cashback'
      AND toStartOfWeek(d.date, 1) = cb.curr_week
),

prev_cb AS (
    SELECT
        sum(amount_usd)           AS volume,
        sum(amount)               AS native_volume,
        sum(activity_count)       AS cnt,
        uniqExact(wallet_address) AS users
    FROM {{ ref('int_execution_gpay_activity_daily') }} d
    CROSS JOIN cashback_bounds cb
    WHERE d.action = 'Cashback'
      AND toStartOfWeek(d.date, 1) = cb.prev_week
)

-- {Action}Volume: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Volume' AS label,
    'All' AS window,
    round(toFloat64(all_volume), 2) AS value,
    toNullable(NULL) AS change_pct
FROM joined

UNION ALL
-- {Action}Volume: 7D (excludes Cashback — handled separately below)
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Volume',
    '7D',
    round(toFloat64(curr_volume), 2),
    round((coalesce(toFloat64(curr_volume) / nullIf(toFloat64(prev_volume), 0), 0) - 1) * 100, 1)
FROM joined
WHERE action_type != 'Cashback'

UNION ALL
-- {Action}Count: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Count',
    'All',
    toFloat64(all_cnt),
    toNullable(NULL)
FROM joined

UNION ALL
-- {Action}Count: 7D (excludes Cashback — handled separately below)
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Count',
    '7D',
    toFloat64(curr_cnt),
    round((coalesce(toFloat64(curr_cnt) / nullIf(toFloat64(prev_cnt), 0), 0) - 1) * 100, 1)
FROM joined
WHERE action_type != 'Cashback'

UNION ALL
-- {Action}Users: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Users',
    'All',
    toFloat64(all_users),
    toNullable(NULL)
FROM joined

UNION ALL
-- {Action}Users: 7D (excludes Cashback — handled separately below)
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Users',
    '7D',
    toFloat64(curr_users),
    round((coalesce(toFloat64(curr_users) / nullIf(toFloat64(prev_users), 0), 0) - 1) * 100, 1)
FROM joined
WHERE action_type != 'Cashback'

UNION ALL
-- CashbackGNO (native GNO amount): All
SELECT 'CashbackGNO', 'All',
    round(toFloat64(all_native), 2),
    toNullable(NULL)
FROM joined
WHERE action_type = 'Cashback'

UNION ALL
-- Cashback 7D metrics: use last distribution week instead of strict 7-day window
-- CashbackVolume: 7D
SELECT 'CashbackVolume', '7D',
    round(toFloat64((SELECT volume FROM curr_cb)), 2),
    round((coalesce(toFloat64((SELECT volume FROM curr_cb)) / nullIf(toFloat64((SELECT volume FROM prev_cb)), 0), 0) - 1) * 100, 1)

UNION ALL
-- CashbackCount: 7D
SELECT 'CashbackCount', '7D',
    toFloat64((SELECT cnt FROM curr_cb)),
    round((coalesce(toFloat64((SELECT cnt FROM curr_cb)) / nullIf(toFloat64((SELECT cnt FROM prev_cb)), 0), 0) - 1) * 100, 1)

UNION ALL
-- CashbackUsers: 7D
SELECT 'CashbackUsers', '7D',
    toFloat64((SELECT users FROM curr_cb)),
    round((coalesce(toFloat64((SELECT users FROM curr_cb)) / nullIf(toFloat64((SELECT users FROM prev_cb)), 0), 0) - 1) * 100, 1)

UNION ALL
-- CashbackGNO: 7D
SELECT 'CashbackGNO', '7D',
    round(toFloat64((SELECT native_volume FROM curr_cb)), 2),
    round((coalesce(toFloat64((SELECT native_volume FROM curr_cb)) / nullIf(toFloat64((SELECT native_volume FROM prev_cb)), 0), 0) - 1) * 100, 1)

UNION ALL
-- TotalBalance (from balances model)
SELECT 'TotalBalance', 'All',
    round(toFloat64(sum(balance_usd)), 2),
    toNullable(NULL)
FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }}
WHERE date = (SELECT max(date) FROM {{ ref('fct_execution_gpay_balances_by_token_daily') }})
  AND symbol IN ('EURe', 'GBPe', 'USDC.e')
