{{ 
    config(
        materialized='view',
        tags=['production','execution','circles','backing']
    )
}}

WITH dates AS (
    SELECT
        min(toDate(block_timestamp)) AS min_date,
        max(toDate(block_timestamp)) AS max_date
    FROM {{ ref('int_execution_circles_backing') }}
    WHERE event_name = 'CirclesBackingCompleted'
),
date_series AS (
    -- generate dense series of dates
    SELECT
        toDate(min_date) + number AS date
    FROM dates
    ARRAY JOIN range(dateDiff('day', min_date, max_date) + 1) AS number
),

filled AS (
    SELECT
        g.date,
        coalesce(t.cnt, 0) AS cnt
    FROM date_series g
    LEFT JOIN (
        SELECT
            toDate(block_timestamp) AS date,
            count() AS cnt
        FROM {{ ref('int_execution_circles_backing') }}
        WHERE event_name = 'CirclesBackingCompleted'
        GROUP BY 1
    ) t
        ON g.date = t.date
)

SELECT
    date,
    cnt,
    SUM(cnt) OVER (ORDER BY date) AS total
FROM filled
WHERE date < today()
