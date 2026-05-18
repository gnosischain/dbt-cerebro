

-- KPI tile: total trust-defined backers (addresses currently trusted by
-- the backers group, var('circles_target_group_address')). WoW delta
-- compares against the same cumulative value 7 days ago.

WITH current_date_d AS (
    SELECT max(date) AS d
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date < today()
),
current AS (
    SELECT cumulative_backers AS value
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date = (SELECT d FROM current_date_d)
),
prior AS (
    SELECT cumulative_backers AS value
    FROM `dbt`.`fct_execution_circles_v2_backers_cumulative_daily`
    WHERE date = (SELECT d FROM current_date_d) - 7
)

SELECT
    c.value                                                          AS value,
    round((c.value - p.value) / nullIf(p.value, 0) * 100, 1)         AS change_pct
FROM current c
CROSS JOIN prior p