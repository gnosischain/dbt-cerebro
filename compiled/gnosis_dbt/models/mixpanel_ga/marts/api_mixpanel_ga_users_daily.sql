

SELECT
    date,
    user_id_hash,
    event_count,
    distinct_event_types,
    distinct_pages
FROM `dbt`.`int_mixpanel_ga_users_daily`
ORDER BY date, event_count DESC