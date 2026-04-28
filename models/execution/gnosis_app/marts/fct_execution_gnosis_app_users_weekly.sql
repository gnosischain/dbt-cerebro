{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','users','mart']
  )
}}

WITH non_onboard AS (
    SELECT
        toStartOfWeek(date, 1) AS week,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
),

onboard AS (
    SELECT
        toStartOfWeek(date, 1) AS week,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind = 'onboard'
),

weekly_activity AS (
    SELECT DISTINCT week, address FROM non_onboard
),

active_weekly AS (
    SELECT week, count(DISTINCT address) AS active_users
    FROM non_onboard
    GROUP BY week
),

new_weekly AS (
    SELECT week, count(DISTINCT address) AS new_users
    FROM onboard
    GROUP BY week
),

returning_weekly AS (
    -- Retained: active this week AND active previous week.
    SELECT
        curr.week                              AS week,
        count(DISTINCT curr.address)           AS returning_users
    FROM weekly_activity curr
    INNER JOIN weekly_activity prev
        ON prev.address = curr.address
       AND prev.week = curr.week - INTERVAL 7 DAY
    GROUP BY curr.week
),

reactivated_weekly AS (
    -- Active this week, NOT active in the prior 4 weeks, but active
    -- earlier. Rewritten as JOIN + LEFT ANTI JOIN — ClickHouse doesn't
    -- support correlated subqueries by default.
    SELECT
        a.week                                 AS week,
        count(DISTINCT a.address)              AS reactivated_users
    FROM (
        SELECT DISTINCT a.week AS week, a.address AS address
        FROM weekly_activity a
        INNER JOIN weekly_activity b
            ON b.address = a.address
           AND b.week < a.week - INTERVAL 28 DAY
    ) a
    LEFT ANTI JOIN weekly_activity mid
        ON mid.address = a.address
       AND mid.week >= a.week - INTERVAL 28 DAY
       AND mid.week < a.week
    GROUP BY a.week
),

date_bounds AS (
    SELECT min(week) AS min_week, toStartOfWeek(today(), 1) AS max_week
    FROM (
        SELECT week FROM active_weekly UNION ALL SELECT week FROM new_weekly
    )
),
calendar AS (
    SELECT addWeeks(min_week, number) AS week
    FROM date_bounds
    ARRAY JOIN range(0, toUInt64(dateDiff('week', min_week, max_week) + 1)) AS number
)

SELECT
    cal.week                                                       AS week,
    coalesce(nw.new_users, 0)                                      AS new_users,
    sum(coalesce(nw.new_users, 0))
        OVER (ORDER BY cal.week
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS cumulative_users,
    coalesce(aw.active_users, 0)                                   AS active_users,
    coalesce(rw.returning_users, 0)                                AS returning_users,
    coalesce(rxw.reactivated_users, 0)                             AS reactivated_users
FROM calendar cal
LEFT JOIN new_weekly nw         ON nw.week = cal.week
LEFT JOIN active_weekly aw      ON aw.week = cal.week
LEFT JOIN returning_weekly rw   ON rw.week = cal.week
LEFT JOIN reactivated_weekly rxw ON rxw.week = cal.week
WHERE cal.week < toStartOfWeek(today(), 1)
ORDER BY cal.week
