

WITH current AS (
    SELECT total AS value
    FROM `dbt`.`fct_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human' AND date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_avatars` WHERE date < today())
),
prior AS (
    SELECT total AS value
    FROM `dbt`.`fct_execution_circles_v2_avatars`
    WHERE avatar_type = 'Human' AND date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_avatars` WHERE date < today()) - 7
)

SELECT
    c.value AS total,
    round((c.value - p.value) / p.value * 100, 1) AS change_pct
FROM current c
CROSS JOIN prior p