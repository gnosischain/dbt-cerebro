{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(avatar_type, date)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'avatars']
    )
}}

WITH daily_counts AS (
    SELECT
        toDate(block_timestamp) AS date,
        avatar_type,
        count() AS cnt
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    GROUP BY 1, 2
),
bounds AS (
    SELECT
        min(date) AS min_date,
        today() AS max_date
    FROM daily_counts
),
avatar_types AS (
    SELECT DISTINCT avatar_type FROM daily_counts
),
calendar AS (
    SELECT
        a.avatar_type,
        addDays(b.min_date, n) AS date
    FROM avatar_types a
    CROSS JOIN bounds b
    ARRAY JOIN range(toUInt32(dateDiff('day', b.min_date, b.max_date) + 1)) AS n
),
dense AS (
    SELECT
        c.date,
        c.avatar_type,
        coalesce(d.cnt, toUInt64(0)) AS cnt
    FROM calendar c
    LEFT JOIN daily_counts d
        ON c.date = d.date
       AND c.avatar_type = d.avatar_type
)

SELECT
    (SELECT min_date FROM bounds) AS min_date,
    (SELECT max_date FROM bounds) AS max_date,
    date,
    avatar_type,
    cnt,
    sum(cnt) OVER (PARTITION BY avatar_type ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total
FROM dense
ORDER BY date, avatar_type
