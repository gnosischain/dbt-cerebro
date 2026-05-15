

SELECT
    backer,
    first_initiated_at,
    last_event_at,
    n_initiated,
    n_completed,
    n_released,
    n_distinct_assets
FROM `dbt`.`int_execution_circles_v2_backing_depositors_current`
ORDER BY first_initiated_at