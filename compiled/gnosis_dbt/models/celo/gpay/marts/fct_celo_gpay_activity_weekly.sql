

-- Weekly rollup of fct_celo_gpay_activity_daily's logic. See that model's header for
-- the safe_address = user grain rationale, and for why funded (ever received money)
-- and activated (ever spent) are two columns rather than one — `cumulative_funded`
-- counted from first PAYMENT until 2026-08-05 and so charted activation.
WITH weekly_activity AS (
    SELECT
        toStartOfWeek(date, 1)  AS week,
        uniqExact(safe_address) AS active_users,
        sum(activity_count)     AS total_payments,
        sum(amount_usd)         AS total_volume_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
      AND toStartOfWeek(date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
),

first_payment AS (
    SELECT safe_address, min(date) AS first_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
    GROUP BY safe_address
),

first_inflow AS (
    SELECT safe_address, min(date) AS first_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action IN ('Top-up', 'Reversal', 'Cashback')
    GROUP BY safe_address
),

weekly_activated AS (
    SELECT toStartOfWeek(first_date, 1) AS week, count() AS n
    FROM first_payment
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
),

weekly_funded AS (
    SELECT toStartOfWeek(first_date, 1) AS week, count() AS n
    FROM first_inflow
    WHERE toStartOfWeek(first_date, 1) < toStartOfWeek(today(), 1)
    GROUP BY week
),

-- Union spine: no funding-only week exists today, but see the daily model — at day
-- grain 9 such periods did, so the shape is kept identical across all three grains.
spine AS (
    SELECT week FROM weekly_activity
    UNION DISTINCT SELECT week FROM weekly_activated
    UNION DISTINCT SELECT week FROM weekly_funded
)

SELECT
    s.week                                                AS week,
    coalesce(a.active_users, 0)                           AS active_users,
    coalesce(a.total_payments, 0)                         AS total_payments,
    round(toFloat64(coalesce(a.total_volume_usd, 0)), 2)  AS total_volume_usd,
    coalesce(fd.n, 0)                                     AS newly_funded,
    sum(coalesce(fd.n, 0)) OVER (ORDER BY s.week)         AS cumulative_funded,
    coalesce(ac.n, 0)                                     AS newly_activated,
    sum(coalesce(ac.n, 0)) OVER (ORDER BY s.week)         AS cumulative_activated
FROM spine s
LEFT JOIN weekly_activity  a  ON a.week  = s.week
LEFT JOIN weekly_funded    fd ON fd.week = s.week
LEFT JOIN weekly_activated ac ON ac.week = s.week
ORDER BY s.week