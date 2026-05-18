

-- KPI tile: new Circles v2 groups registered in the last 7 days, with
-- week-over-week change. Sourced from int_execution_circles_v2_groups_overview_daily
-- (rather than the api_ view) so the source-of-truth aggregate isn't
-- filtered to date < today() — we want yesterday too.

WITH windowed AS (
    SELECT
        sumIf(n_new_groups, date >  today() - 7 AND date <= today()) AS value,
        sumIf(n_new_groups, date >  today() - 14 AND date <= today() - 7) AS prior_value
    FROM `dbt`.`int_execution_circles_v2_groups_overview_daily`
    WHERE date > today() - 14
)

SELECT
    value                                                                   AS value,
    round((value - prior_value) / nullIf(prior_value, 0) * 100, 1)          AS change_pct
FROM windowed