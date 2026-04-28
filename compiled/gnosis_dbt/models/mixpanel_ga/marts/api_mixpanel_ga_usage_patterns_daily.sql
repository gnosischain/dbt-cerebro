

SELECT
    date,
    hour_of_day,
    day_of_week,
    event_count,
    unique_users
FROM `dbt`.`int_mixpanel_ga_usage_patterns_daily`
ORDER BY date, hour_of_day, day_of_week