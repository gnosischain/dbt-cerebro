{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(activity_kind, cohort_month, activity_month)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','retention','mart']
  )
}}

WITH cohort_user AS (
    -- Cohort month defined as the month of the user's GLOBAL first-seen
    -- ('onboard' row), same as fct_execution_gnosis_app_retention_monthly.
    -- This keeps the cohort denominator consistent across action types.
    SELECT
        address                       AS address,
        toStartOfMonth(date)          AS cohort_month
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind = 'onboard'
),

monthly_activity_by_kind AS (
    SELECT
        address,
        activity_kind,
        toStartOfMonth(date) AS activity_month
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY address, activity_kind, activity_month
),

cohort_activity AS (
    SELECT
        a.activity_kind,
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.address)                           AS users
    FROM cohort_user f
    INNER JOIN monthly_activity_by_kind a ON f.address = a.address
    WHERE a.activity_month >= f.cohort_month
    GROUP BY a.activity_kind, f.cohort_month, a.activity_month
),

cohort_size AS (
    -- True denominator: the onboard-cohort size (distinct users who onboarded
    -- in cohort_month). The users doing an action in any month are a subset of
    -- this, so retention_pct is bounded at 100% and is consistent across
    -- actions. NOTE: do NOT anchor on the month-0 ACTION adopters here -- the
    -- cohort is the onboard cohort and the numerator counts ANY cohort user
    -- doing the action each month (not a fixed followed subset), so more users
    -- adopt the action in later months than in month 0, which pushes a
    -- month-0-anchored ratio above 100%.
    SELECT
        cohort_month,
        count(DISTINCT address) AS initial_users
    FROM cohort_user
    GROUP BY cohort_month
)

SELECT
    a.activity_kind,
    a.cohort_month,
    a.activity_month,
    a.months_since,
    a.users,
    s.initial_users,
    round(a.users / nullIf(s.initial_users, 0) * 100, 1) AS retention_pct
FROM cohort_activity a
INNER JOIN cohort_size s ON a.cohort_month = s.cohort_month
ORDER BY a.activity_kind, a.cohort_month, a.activity_month
