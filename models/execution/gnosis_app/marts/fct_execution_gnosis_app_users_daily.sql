{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','users','mart']
  )
}}

{# Description in schema.yml — see fct_execution_gnosis_app_users_daily #}

WITH non_onboard AS (
    -- All real-activity rows (exclude synthetic 'onboard' rows so we don't
    -- double-count a user as active via their onboard marker alone).
    SELECT
        date,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind != 'onboard'
),

onboard AS (
    SELECT
        date,
        address
    FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
    WHERE activity_kind = 'onboard'
),

distinct_daily AS (
    SELECT DISTINCT date, address FROM non_onboard
),

active_daily AS (
    SELECT
        date,
        count(DISTINCT address) AS active_users
    FROM non_onboard
    GROUP BY date
),

new_daily AS (
    SELECT
        date,
        count(DISTINCT address) AS new_users
    FROM onboard
    GROUP BY date
),

-- Dense calendar spine so cumulative series stays continuous.
date_bounds AS (
    SELECT min(date) AS min_date, today() AS max_date
    FROM (
        SELECT date FROM active_daily
        UNION ALL SELECT date FROM new_daily
    )
),
calendar AS (
    SELECT addDays(min_date, number) AS date
    FROM date_bounds
    ARRAY JOIN range(0, toUInt64(dateDiff('day', min_date, max_date) + 1)) AS number
),

-- Returning: active today AND had any activity in the prior 7 days
-- (excluding today). Rewritten as an INNER JOIN — ClickHouse doesn't
-- support correlated subqueries by default.
returning_daily AS (
    SELECT
        a.date                                 AS date,
        count(DISTINCT a.address)              AS returning_users
    FROM distinct_daily a
    INNER JOIN distinct_daily b
        ON b.address = a.address
       AND b.date >= a.date - INTERVAL 7 DAY
       AND b.date < a.date
    GROUP BY a.date
),

-- Reactivated: active today, inactive for the prior 30 days, but had
-- activity before that. INNER JOIN for the "had activity before 30d"
-- condition; LEFT ANTI JOIN for the "no activity in the prior 30d" one.
reactivated_daily AS (
    SELECT
        a.date                                 AS date,
        count(DISTINCT a.address)              AS reactivated_users
    FROM (
        SELECT DISTINCT a.date AS date, a.address AS address
        FROM distinct_daily a
        INNER JOIN distinct_daily b
            ON b.address = a.address
           AND b.date < a.date - INTERVAL 30 DAY
    ) a
    LEFT ANTI JOIN distinct_daily mid
        ON mid.address = a.address
       AND mid.date >= a.date - INTERVAL 30 DAY
       AND mid.date < a.date
    GROUP BY a.date
)

SELECT
    cal.date                                                       AS date,
    coalesce(nd.new_users, 0)                                      AS new_users,
    sum(coalesce(nd.new_users, 0))
        OVER (ORDER BY cal.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)    AS cumulative_users,
    coalesce(ad.active_users, 0)                                   AS active_users,
    coalesce(rd.returning_users, 0)                                AS returning_users,
    coalesce(rx.reactivated_users, 0)                              AS reactivated_users
FROM calendar cal
LEFT JOIN new_daily nd          ON nd.date = cal.date
LEFT JOIN active_daily ad       ON ad.date = cal.date
LEFT JOIN returning_daily rd    ON rd.date = cal.date
LEFT JOIN reactivated_daily rx  ON rx.date = cal.date
ORDER BY cal.date
