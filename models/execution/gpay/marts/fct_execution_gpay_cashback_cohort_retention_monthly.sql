{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cohort_month, activity_month)',
    tags=['production','execution','gpay']
  )
}}

-- Cohort = first month wallet received cashback
WITH first_cashback AS (
    SELECT
        wallet_address,
        toStartOfMonth(min(date)) AS cohort_month
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Cashback'
    GROUP BY wallet_address
),

-- Track their PAYMENT behaviour in each subsequent month
monthly_payments AS (
    SELECT
        wallet_address,
        toStartOfMonth(date) AS activity_month,
        sum(amount_usd)      AS amount_usd
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Payment'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY wallet_address, activity_month
),

-- Join: only cashback recipients, then aggregate by cohort × month
cohort_activity AS (
    SELECT
        f.cohort_month,
        m.activity_month,
        dateDiff('month', f.cohort_month, m.activity_month) AS months_since,
        count(DISTINCT m.wallet_address)                     AS users,
        sum(m.amount_usd)                                    AS amount_usd
    FROM first_cashback f
    INNER JOIN monthly_payments m
        ON  m.wallet_address = f.wallet_address
        AND m.activity_month >= f.cohort_month
    GROUP BY f.cohort_month, m.activity_month
),

with_initial AS (
    SELECT
        *,
        max(users) OVER (PARTITION BY cohort_month)                          AS initial_users,
        argMin(amount_usd, activity_month) OVER (PARTITION BY cohort_month)  AS initial_amount_usd
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
    round(toFloat64(amount_usd), 2)                  AS amount_usd
FROM with_initial
ORDER BY cohort_month, activity_month
