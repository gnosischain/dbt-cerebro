

SELECT
    date,
    event_name,
    event_category,
    event_count,
    unique_users,
    unique_devices,
    autocapture_ratio
FROM `dbt`.`int_mixpanel_ga_events_daily`
ORDER BY date, event_name