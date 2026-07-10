

-- Per-action monthly cohort retention. Mirrors
-- fct_execution_gpay_retention_by_action_monthly with safe_address grain.
-- Cohort = month a card first performed the given action.
WITH first_action AS (
    SELECT
        safe_address,
        action,
        toStartOfMonth(min(date)) AS cohort_month
    FROM `dbt`.`int_celo_gpay_activity_daily`
    GROUP BY safe_address, action
),

monthly_activity AS (
    SELECT
        safe_address,
        action,
        toStartOfMonth(date) AS activity_month,
        sum(amount_usd)      AS amount_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY safe_address, action, activity_month
),

cohort_activity AS (
    SELECT
        f.action,
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.safe_address)                       AS users,
        sum(a.amount_usd)                                    AS amount_usd
    FROM first_action f
    INNER JOIN monthly_activity a
        ON  a.safe_address = f.safe_address
        AND a.action       = f.action
    GROUP BY f.action, f.cohort_month, a.activity_month
),

with_initial AS (
    SELECT
        *,
        max(users) OVER (PARTITION BY action, cohort_month)                        AS initial_users,
        argMin(amount_usd, activity_month) OVER (PARTITION BY action, cohort_month) AS initial_amount_usd
    FROM cohort_activity
)

SELECT
    action,
    cohort_month,
    activity_month,
    months_since,
    users,
    initial_users,
    round(users / initial_users * 100, 1)           AS retention_pct,
    round(amount_usd / initial_amount_usd * 100, 1) AS amount_retention_pct,
    round(toFloat64(amount_usd), 2)                 AS amount_usd
FROM with_initial
ORDER BY action, cohort_month, activity_month