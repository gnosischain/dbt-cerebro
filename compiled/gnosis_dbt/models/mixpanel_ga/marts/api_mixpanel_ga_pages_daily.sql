

SELECT
    date,
    current_domain,
    page_path,
    event_count,
    unique_users
FROM `dbt`.`int_mixpanel_ga_pages_daily`
ORDER BY date, event_count DESC