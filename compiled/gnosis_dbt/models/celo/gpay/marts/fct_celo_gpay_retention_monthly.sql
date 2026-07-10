

-- Monthly payment-cohort retention (users + USD). Mirrors
-- fct_execution_gpay_retention_monthly with safe_address as the user grain.
-- Cohort = month of a card's first Payment.
WITH first_payment AS (
    SELECT
        safe_address,
        toStartOfMonth(min(date)) AS cohort_month
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
    GROUP BY safe_address
),

monthly_activity AS (
    SELECT
        safe_address,
        toStartOfMonth(date) AS activity_month,
        sum(amount_usd)      AS amount_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY safe_address, activity_month
),

cohort_activity AS (
    SELECT
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.safe_address) AS users,
        sum(a.amount_usd)              AS amount_usd
    FROM first_payment f
    INNER JOIN monthly_activity a ON f.safe_address = a.safe_address
    GROUP BY f.cohort_month, a.activity_month
),

with_initial AS (
    SELECT
        *,
        max(users) OVER (PARTITION BY cohort_month) AS initial_users,
        argMin(amount_usd, activity_month) OVER (PARTITION BY cohort_month) AS initial_amount_usd
    FROM cohort_activity
)

SELECT
    cohort_month,
    activity_month,
    months_since,
    users,
    initial_users,
    round(users / initial_users * 100, 1)           AS retention_pct,
    round(amount_usd / initial_amount_usd * 100, 1) AS amount_retention_pct,
    round(toFloat64(amount_usd), 2)                 AS amount_usd
FROM with_initial
ORDER BY cohort_month, activity_month