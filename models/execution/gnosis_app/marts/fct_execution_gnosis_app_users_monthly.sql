{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','users','mart']
  )
}}

WITH non_onboard AS (
    SELECT
        toStartOfMonth(date) AS month,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
),

onboard AS (
    SELECT
        toStartOfMonth(date) AS month,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind = 'onboard'
),

monthly_activity AS (
    SELECT DISTINCT month, address FROM non_onboard
),

active_monthly AS (
    SELECT month, count(DISTINCT address) AS active_users
    FROM non_onboard
    GROUP BY month
),

new_monthly AS (
    SELECT month, count(DISTINCT address) AS new_users
    FROM onboard
    GROUP BY month
),

returning_monthly AS (
    SELECT
        curr.month                             AS month,
        count(DISTINCT curr.address)           AS returning_users
    FROM monthly_activity curr
    INNER JOIN monthly_activity prev
        ON prev.address = curr.address
       AND prev.month = subtractMonths(curr.month, 1)
    GROUP BY curr.month
),

reactivated_monthly AS (
    -- Active this month, NOT active in the prior 2 months, but active
    -- earlier. Rewritten as JOIN + LEFT ANTI JOIN — ClickHouse doesn't
    -- support correlated subqueries by default.
    SELECT
        a.month                                AS month,
        count(DISTINCT a.address)              AS reactivated_users
    FROM (
        SELECT DISTINCT a.month AS month, a.address AS address
        FROM monthly_activity a
        INNER JOIN monthly_activity b
            ON b.address = a.address
           AND b.month < subtractMonths(a.month, 2)
    ) a
    LEFT ANTI JOIN monthly_activity mid
        ON mid.address = a.address
       AND mid.month >= subtractMonths(a.month, 2)
       AND mid.month < a.month
    GROUP BY a.month
),

date_bounds AS (
    SELECT min(month) AS min_month, toStartOfMonth(today()) AS max_month
    FROM (
        SELECT month FROM active_monthly UNION ALL SELECT month FROM new_monthly
    )
),
calendar AS (
    SELECT addMonths(min_month, number) AS month
    FROM date_bounds
    ARRAY JOIN range(0, toUInt64(dateDiff('month', min_month, max_month) + 1)) AS number
)

SELECT
    cal.month                                                      AS month,
    coalesce(nm.new_users, 0)                                      AS new_users,
    sum(coalesce(nm.new_users, 0))
        OVER (ORDER BY cal.month
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS cumulative_users,
    coalesce(am.active_users, 0)                                   AS active_users,
    coalesce(rm.returning_users, 0)                                AS returning_users,
    coalesce(rxm.reactivated_users, 0)                             AS reactivated_users
FROM calendar cal
LEFT JOIN new_monthly nm        ON nm.month = cal.month
LEFT JOIN active_monthly am     ON am.month = cal.month
LEFT JOIN returning_monthly rm  ON rm.month = cal.month
LEFT JOIN reactivated_monthly rxm ON rxm.month = cal.month
ORDER BY cal.month
