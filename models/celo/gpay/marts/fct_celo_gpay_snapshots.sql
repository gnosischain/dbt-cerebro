{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(window, label)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay']
  )
}}

-- All-time + trailing-7D headline snapshots per action, plus TotalBalance.
-- Mirrors fct_execution_gpay_snapshots but drops every Cashback branch (no
-- GNO cashback program on Celo) and uses safe_address as the user grain.
-- Label pattern: replaceAll(action,' '/'-') || {Volume,Count,Users}, e.g.
-- 'Top-up' -> 'TopupVolume'. Actions present: Payment, Top-up, Withdrawal,
-- Reversal. 7D change_pct compares the trailing 7 days vs the prior 7 days.
WITH wd AS (
    SELECT max(date) AS max_date
    FROM {{ ref('int_celo_gpay_activity_daily') }}
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
        sum(amount_usd)         AS volume,
        sum(amount)             AS native_volume,
        sum(activity_count)     AS cnt,
        uniqExact(safe_address) AS users
    FROM {{ ref('int_celo_gpay_activity_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.curr_start AND d.date <= b.curr_end
    GROUP BY action
),

prev_7d AS (
    SELECT
        action,
        sum(amount_usd)         AS volume,
        sum(amount)             AS native_volume,
        sum(activity_count)     AS cnt,
        uniqExact(safe_address) AS users
    FROM {{ ref('int_celo_gpay_activity_daily') }} d
    CROSS JOIN bounds b
    WHERE d.date > b.prev_start AND d.date <= b.prev_end
    GROUP BY action
),

all_time AS (
    SELECT
        action,
        sum(amount_usd)         AS volume,
        sum(amount)             AS native_volume,
        sum(activity_count)     AS cnt,
        uniqExact(safe_address) AS users
    FROM {{ ref('int_celo_gpay_activity_daily') }}
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
)

-- {Action}Volume: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Volume' AS label,
    'All' AS window,
    round(toFloat64(all_volume), 2) AS value,
    toNullable(NULL) AS change_pct
FROM joined

UNION ALL
-- {Action}Volume: 7D
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Volume',
    '7D',
    round(toFloat64(curr_volume), 2),
    round((coalesce(toFloat64(curr_volume) / nullIf(toFloat64(prev_volume), 0), 0) - 1) * 100, 1)
FROM joined

UNION ALL
-- {Action}Count: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Count',
    'All',
    toFloat64(all_cnt),
    toNullable(NULL)
FROM joined

UNION ALL
-- {Action}Count: 7D
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Count',
    '7D',
    toFloat64(curr_cnt),
    round((coalesce(toFloat64(curr_cnt) / nullIf(toFloat64(prev_cnt), 0), 0) - 1) * 100, 1)
FROM joined

UNION ALL
-- {Action}Users: All
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Users',
    'All',
    toFloat64(all_users),
    toNullable(NULL)
FROM joined

UNION ALL
-- {Action}Users: 7D
SELECT
    replaceAll(replaceAll(action_type, ' ', ''), '-', '') || 'Users',
    '7D',
    toFloat64(curr_users),
    round((coalesce(toFloat64(curr_users) / nullIf(toFloat64(prev_users), 0), 0) - 1) * 100, 1)
FROM joined

UNION ALL
-- TotalBalance: All (spendable stablecoin float, mark-to-market, latest day)
--
-- Partitioned on token_class, NOT on a hardcoded symbol list. The previous
-- `symbol IN ('USDC','USDT')` dropped nothing (those are the only tokens with flow)
-- but would have silently excluded any newly-transacting whitelisted token from the
-- headline with no error. Driving it off celo_tokens_whitelist.token_class means a
-- token added to the seed lands in the right bucket automatically.
--
-- STABLECOIN only, because this tile means SPENDABLE float. An RWA reward token is
-- not spendable at the bridge — int_celo_gpay_activity classifies a non-stablecoin
-- transfer to the settlement contract as 'Other', never 'Payment' — so blending gold
-- cashback into this number would overstate purchasing power. RWA is reported
-- alongside as RewardBalance rather than dropped, so it can never go invisible.
--
-- if(count() = 0, 0, ...) distinguishes "no position" (a true zero) from "position
-- held but unpriced" (sum returns NULL, which stays NULL). A NULL tile is a visible
-- failure; a $0 tile is a plausible-looking lie.
SELECT 'TotalBalance', 'All',
    if(count() = 0, 0, round(toFloat64(sum(balance_usd)), 2)),
    toNullable(NULL)
FROM {{ ref('fct_celo_gpay_balances_by_token_daily') }}
WHERE date = (SELECT max(date) FROM {{ ref('fct_celo_gpay_balances_by_token_daily') }})
  AND token_class = 'STABLECOIN'

UNION ALL
-- FundedCards: All (cards that have ever RECEIVED money)
--
-- Distinct from PaymentUsers, which counts cards that have ever SPENT. The two were
-- conflated until 2026-08-05: api_celo_gpay_total_funded served PaymentUsers, so the
-- "funded" tile read 662 when 1087 cards had actually been funded. Inbound actions
-- only — Cashback is currently compiled out of int_celo_gpay_activity and Reversal has
-- never fired, so this is Top-up in practice, but naming all three keeps it correct
-- once either starts. Funnel: issued (registry) > FundedCards > PaymentUsers.
SELECT 'FundedCards', 'All',
    toFloat64(uniqExact(safe_address)),
    toNullable(NULL)
FROM {{ ref('int_celo_gpay_activity_daily') }}
WHERE action IN ('Top-up', 'Reversal', 'Cashback')

UNION ALL
-- RewardBalance: All (RWA cashback holdings, mark-to-market, latest day)
-- Zero until the cashback program pays out. Kept as an explicit line so the first
-- reward token to land is visible instead of being filtered away. NOTE: until
-- cashback_sources is populated in int_celo_gpay_activity, a reward inflow still
-- misclassifies as 'Top-up' — this tile shows the holding, not the correct action.
SELECT 'RewardBalance', 'All',
    if(count() = 0, 0, round(toFloat64(sum(balance_usd)), 2)),
    toNullable(NULL)
FROM {{ ref('fct_celo_gpay_balances_by_token_daily') }}
WHERE date = (SELECT max(date) FROM {{ ref('fct_celo_gpay_balances_by_token_daily') }})
  AND token_class = 'RWA'
