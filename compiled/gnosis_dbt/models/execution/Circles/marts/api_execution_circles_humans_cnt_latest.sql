

WITH

humans_latest AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_avatars`
    WHERE 
        date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_avatars`)
        AND avatar_type = 'Human'
),

humans_7d AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_avatars`
    WHERE 
        date = subtractDays((SELECT max(date) FROM `dbt`.`fct_execution_circles_avatars`), 7)
        AND avatar_type = 'Human'
)

SELECT
    t1.total AS total
    ,IF(t1.total=0 AND t2.total=0, 0, ROUND((COALESCE(t1.total / NULLIF(t2.total, 0), 0) - 1) * 100, 1)) AS change_pct
FROM humans_latest t1
CROSS JOIN humans_7d t2