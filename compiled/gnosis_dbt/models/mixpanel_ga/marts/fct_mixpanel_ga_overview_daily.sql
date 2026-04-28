

WITH daily_dau AS (
    SELECT
        date,
        count()                     AS dau,
        sum(event_count)            AS total_events
    FROM `dbt`.`int_mixpanel_ga_users_daily`
    GROUP BY date
),

daily_devices AS (
    SELECT
        date,
        sum(unique_devices)         AS unique_devices
    FROM `dbt`.`int_mixpanel_ga_events_daily`
    GROUP BY date
),

daily_event_types AS (
    SELECT
        date,
        count()                     AS distinct_event_types,
        sum(event_count * autocapture_ratio)  AS autocapture_events,
        sum(event_count * (1 - autocapture_ratio)) AS custom_events
    FROM `dbt`.`int_mixpanel_ga_events_daily`
    GROUP BY date
),

daily_countries AS (
    SELECT
        date,
        uniqExact(country_code)     AS distinct_countries
    FROM `dbt`.`int_mixpanel_ga_geo_daily`
    GROUP BY date
),

daily_referrers AS (
    SELECT
        date,
        uniqExact(referrer_domain)  AS distinct_referrers
    FROM `dbt`.`int_mixpanel_ga_traffic_daily`
    WHERE referrer_domain != 'direct'
    GROUP BY date
),

first_seen AS (
    SELECT
        user_id_hash,
        min(date) AS first_date
    FROM `dbt`.`int_mixpanel_ga_users_daily`
    GROUP BY user_id_hash
),

daily_new_users AS (
    SELECT
        first_date AS date,
        count()    AS new_users
    FROM first_seen
    GROUP BY first_date
)

SELECT
    d.date                                                  AS date,
    d.total_events                                          AS total_events,
    d.dau                                                   AS dau,
    COALESCE(dd.unique_devices, 0)                          AS unique_devices,
    COALESCE(dn.new_users, 0)                               AS new_users,
    sum(COALESCE(dn.new_users, 0)) OVER (ORDER BY d.date)   AS cumulative_users,
    COALESCE(det.distinct_event_types, 0)                   AS distinct_event_types,
    COALESCE(dc.distinct_countries, 0)                      AS distinct_countries,
    COALESCE(dr.distinct_referrers, 0)                      AS distinct_referrers,
    round(COALESCE(det.autocapture_events, 0))              AS autocapture_events,
    round(COALESCE(det.custom_events, 0))                   AS custom_events,
    round(d.total_events / greatest(d.dau, 1), 2)          AS avg_events_per_user
FROM daily_dau d
LEFT JOIN daily_devices dd ON d.date = dd.date
LEFT JOIN daily_event_types det ON d.date = det.date
LEFT JOIN daily_countries dc ON d.date = dc.date
LEFT JOIN daily_referrers dr ON d.date = dr.date
LEFT JOIN daily_new_users dn ON d.date = dn.date
ORDER BY date