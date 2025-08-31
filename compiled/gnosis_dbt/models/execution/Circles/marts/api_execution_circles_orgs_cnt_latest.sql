

WITH

orgs_latest AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_avatars`
    WHERE 
        date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_avatars`)
        AND avatar_type = 'Org'
),

orgs_7d AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_avatars`
    WHERE 
        date = subtractDays((SELECT max(date) FROM `dbt`.`fct_execution_circles_avatars`), 7)
        AND avatar_type = 'Org'
)

SELECT
    t1.total AS total
    ,IF(t1.total=0 AND t2.total=0, 0, ROUND((COALESCE(t1.total / NULLIF(t2.total, 0), 0) - 1) * 100, 1)) AS change_pct
FROM orgs_latest t1
CROSS JOIN orgs_7d t2