

SELECT
    date,
    browser,
    os,
    device_type,
    event_count,
    unique_users
FROM `dbt`.`int_mixpanel_ga_tech_daily`
ORDER BY date, event_count DESC