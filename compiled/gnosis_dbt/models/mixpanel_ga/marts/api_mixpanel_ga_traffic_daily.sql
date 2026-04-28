

SELECT
    date,
    referrer_domain,
    initial_referrer_domain,
    event_count,
    unique_users
FROM `dbt`.`int_mixpanel_ga_traffic_daily`
ORDER BY date, event_count DESC