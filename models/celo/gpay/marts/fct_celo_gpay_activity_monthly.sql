{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month)',
    tags=['production','celo','gpay']
  )
}}

-- Monthly rollup of fct_celo_gpay_activity_daily's logic. See that model's header for
-- the safe_address = user grain rationale, and for why funded (ever received money)
-- and activated (ever spent) are two columns rather than one — `cumulative_funded`
-- counted from first PAYMENT until 2026-08-05 and so charted activation.
WITH monthly_activity AS (
    SELECT
        toStartOfMonth(date)    AS month,
        uniqExact(safe_address) AS active_users,
        sum(activity_count)     AS total_payments,
        sum(amount_usd)         AS total_volume_usd
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action = 'Payment'
      AND toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY month
),

first_payment AS (
    SELECT safe_address, min(date) AS first_date
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action = 'Payment'
    GROUP BY safe_address
),

first_inflow AS (
    SELECT safe_address, min(date) AS first_date
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action IN ('Top-up', 'Reversal', 'Cashback')
    GROUP BY safe_address
),

monthly_activated AS (
    SELECT toStartOfMonth(first_date) AS month, count() AS n
    FROM first_payment
    WHERE toStartOfMonth(first_date) < toStartOfMonth(today())
    GROUP BY month
),

monthly_funded AS (
    SELECT toStartOfMonth(first_date) AS month, count() AS n
    FROM first_inflow
    WHERE toStartOfMonth(first_date) < toStartOfMonth(today())
    GROUP BY month
),

-- Union spine: no funding-only month exists today, but see the daily model — at day
-- grain 9 such periods did, so the shape is kept identical across all three grains.
spine AS (
    SELECT month FROM monthly_activity
    UNION DISTINCT SELECT month FROM monthly_activated
    UNION DISTINCT SELECT month FROM monthly_funded
)

SELECT
    s.month                                               AS month,
    coalesce(a.active_users, 0)                           AS active_users,
    coalesce(a.total_payments, 0)                         AS total_payments,
    round(toFloat64(coalesce(a.total_volume_usd, 0)), 2)  AS total_volume_usd,
    coalesce(fd.n, 0)                                     AS newly_funded,
    sum(coalesce(fd.n, 0)) OVER (ORDER BY s.month)        AS cumulative_funded,
    coalesce(ac.n, 0)                                     AS newly_activated,
    sum(coalesce(ac.n, 0)) OVER (ORDER BY s.month)        AS cumulative_activated
FROM spine s
LEFT JOIN monthly_activity  a  ON a.month  = s.month
LEFT JOIN monthly_funded    fd ON fd.month = s.month
LEFT JOIN monthly_activated ac ON ac.month = s.month
ORDER BY s.month
