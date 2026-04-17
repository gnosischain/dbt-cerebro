

WITH cohort_user AS (
    -- Cohort month defined as the month of the user's GLOBAL first-seen
    -- ('onboard' row), same as fct_execution_gnosis_app_retention_monthly.
    -- This keeps the cohort denominator consistent across action types.
    SELECT
        address                       AS address,
        toStartOfMonth(date)          AS cohort_month
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
    WHERE activity_kind = 'onboard'
),

monthly_activity_by_kind AS (
    SELECT
        address,
        activity_kind,
        toStartOfMonth(date) AS activity_month
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
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

with_initial AS (
    SELECT
        *,
        max(users) OVER (PARTITION BY activity_kind, cohort_month) AS initial_users
    FROM cohort_activity
)

SELECT
    activity_kind,
    cohort_month,
    activity_month,
    months_since,
    users,
    initial_users,
    round(users / greatest(initial_users, 1) * 100, 1) AS retention_pct
FROM with_initial
ORDER BY activity_kind, cohort_month, activity_month