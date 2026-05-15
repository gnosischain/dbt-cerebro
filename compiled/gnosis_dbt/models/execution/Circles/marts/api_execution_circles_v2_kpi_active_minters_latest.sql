

-- KPI tile: latest-day Active Minters count with week-over-week change.

WITH current AS (
    SELECT active_minters AS value
    FROM `dbt`.`fct_execution_circles_v2_active_minters_daily`
    WHERE date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_active_minters_daily` WHERE date < today())
),
prior AS (
    SELECT active_minters AS value
    FROM `dbt`.`fct_execution_circles_v2_active_minters_daily`
    WHERE date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_active_minters_daily` WHERE date < today()) - 7
)

SELECT
    c.value AS value,
    round((c.value - p.value) / nullIf(p.value, 0) * 100, 1) AS change_pct
FROM current c
CROSS JOIN prior p