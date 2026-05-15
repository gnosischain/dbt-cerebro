

SELECT
    date,
    lifecycle_stage,
    n_events,
    n_distinct_backers,
    n_distinct_assets
FROM `dbt`.`int_execution_circles_v2_backing_events_daily`
WHERE date < today()
ORDER BY date DESC, lifecycle_stage