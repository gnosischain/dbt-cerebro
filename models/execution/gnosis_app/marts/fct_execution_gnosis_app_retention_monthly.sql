{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cohort_month, activity_month)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','retention','mart']
  )
}}

WITH cohort_user AS (
    -- cohort_month = month of the user's 'onboard' row (first-seen in GA).
    SELECT
        address                       AS address,
        toStartOfMonth(date)          AS cohort_month
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind = 'onboard'
),

monthly_activity AS (
    -- Any real-activity month for this user (exclude synthetic onboard).
    SELECT
        address,
        toStartOfMonth(date) AS activity_month,
        sum(coalesce(amount_usd, 0)) AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY address, activity_month
),

cohort_activity AS (
    SELECT
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.address)                           AS users,
        sum(a.amount_usd)                                   AS amount_usd
    FROM cohort_user f
    INNER JOIN monthly_activity a ON f.address = a.address
    -- Only count activity from the cohort month onwards. Without this,
    -- users who had on-chain activity (swaps, etc.) BEFORE their GA
    -- onboard produce rows in the upper triangle (activity_month <
    -- cohort_month) with spurious 0% retention values.
    WHERE a.activity_month >= f.cohort_month
    GROUP BY f.cohort_month, a.activity_month
),

cohort_size AS (
    -- Onboard-cohort size: distinct users who onboarded in cohort_month.
    -- Bounds retention at 100% (active users are a subset of the cohort). See
    -- fct_execution_gnosis_app_retention_by_action_monthly for why a month-0
    -- anchor is wrong for an onboard-cohort numerator.
    SELECT
        cohort_month,
        count(DISTINCT address) AS initial_users
    FROM cohort_user
    GROUP BY cohort_month
),

with_amount AS (
    SELECT
        *,
        argMin(amount_usd, activity_month) OVER (PARTITION BY cohort_month)   AS initial_amount_usd
    FROM cohort_activity
)

SELECT
    w.cohort_month,
    w.activity_month,
    w.months_since,
    w.users,
    s.initial_users,
    round(w.users / nullIf(s.initial_users, 0) * 100, 1)                         AS retention_pct,
    round(w.amount_usd / nullIf(w.initial_amount_usd, 0) * 100, 1)               AS amount_retention_pct,
    round(toFloat64(w.amount_usd), 2)                                            AS amount_usd
FROM with_amount w
INNER JOIN cohort_size s ON w.cohort_month = s.cohort_month
ORDER BY w.cohort_month, w.activity_month
