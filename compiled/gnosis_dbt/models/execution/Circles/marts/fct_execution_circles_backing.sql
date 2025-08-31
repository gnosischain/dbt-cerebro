

WITH dates AS (
    -- get min/max date 
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM `dbt`.`int_execution_circles_backing`
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
    LEFT JOIN `dbt`.`int_execution_circles_backing` t
        ON g.date = t.date
)

SELECT
    date,
    cnt,
    SUM(cnt) OVER (ORDER BY date) AS total
FROM filled
WHERE date < today()