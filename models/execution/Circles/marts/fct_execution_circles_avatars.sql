{{ 
    config(
        materialized='view',
        tags=['production','execution','circles','avatars']
    )
}}

WITH dates AS (
    SELECT
        min(toDate(block_timestamp)) AS min_date,
        max(toDate(block_timestamp)) AS max_date
    FROM {{ ref('int_execution_circles_v2_avatars') }}
),
date_series AS (
    SELECT
        toDate(min_date) + number AS date
    FROM dates
    ARRAY JOIN range(dateDiff('day', min_date, max_date) + 1) AS number
),
avatar_types AS (
    SELECT DISTINCT avatar_type
    FROM {{ ref('int_execution_circles_v2_avatars') }}
),
dailies AS (
    SELECT
        toDate(block_timestamp) AS date,
        avatar_type,
        count() AS cnt
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    GROUP BY 1, 2
),
dense_grid AS (
    SELECT
        d.date,
        a.avatar_type
    FROM date_series d
    CROSS JOIN avatar_types a
),
filled AS (
    SELECT
        g.date,
        g.avatar_type,
        coalesce(t.cnt, 0) AS cnt
    FROM dense_grid g
    LEFT JOIN dailies t
        ON g.date = t.date
       AND g.avatar_type = t.avatar_type
)

SELECT
    date,
    avatar_type,
    cnt,
    SUM(cnt) OVER (PARTITION BY avatar_type ORDER BY date) AS total
FROM filled
