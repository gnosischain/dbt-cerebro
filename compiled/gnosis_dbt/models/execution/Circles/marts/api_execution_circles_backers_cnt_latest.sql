WITH

backers_latest AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_backing`
    WHERE 
        date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_backing`)
),

backers_7d AS (
    SELECT 
        total
    FROM `dbt`.`fct_execution_circles_backing`
    WHERE 
        date = subtractDays((SELECT max(date) FROM `dbt`.`fct_execution_circles_backing`), 7)
)

SELECT
    t1.total AS total
    ,IF(t1.total=0 AND t2.total=0, 0, ROUND((1- COALESCE(t2.total / NULLIF(t1.total, 0), 0)) * 100, 1)) AS change_pct
FROM backers_latest t1
CROSS JOIN backers_7d t2