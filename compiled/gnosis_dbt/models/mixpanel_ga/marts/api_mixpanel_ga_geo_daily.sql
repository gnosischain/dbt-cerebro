

-- Rolled up to country level (no region) for privacy
SELECT
    date,
    country_code,
    sum(event_count)                AS event_count,
    sum(unique_users)               AS unique_users,
    sum(unique_devices)             AS unique_devices
FROM `dbt`.`int_mixpanel_ga_geo_daily`
GROUP BY date, country_code
ORDER BY date, event_count DESC