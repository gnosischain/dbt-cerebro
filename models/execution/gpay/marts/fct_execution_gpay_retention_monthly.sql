{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cohort_month, activity_month)',
    tags=['production','execution','gpay']
  )
}}

WITH first_payment AS (
    SELECT
        wallet_address,
        toStartOfMonth(min(date)) AS cohort_month
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Payment'
    GROUP BY wallet_address
),

monthly_activity AS (
    SELECT
        wallet_address,
        toStartOfMonth(date) AS activity_month,
        sum(amount_usd)      AS amount_usd
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Payment'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY wallet_address, activity_month
),

cohort_activity AS (
    SELECT
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.wallet_address) AS users,
        sum(a.amount_usd)               AS amount_usd
    FROM first_payment f
    INNER JOIN monthly_activity a ON f.wallet_address = a.wallet_address
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
    round(users / initial_users * 100, 1) AS retention_pct,
    round(amount_usd / initial_amount_usd * 100, 1) AS amount_retention_pct,
    round(toFloat64(amount_usd), 2)       AS amount_usd
FROM with_initial
ORDER BY cohort_month, activity_month
