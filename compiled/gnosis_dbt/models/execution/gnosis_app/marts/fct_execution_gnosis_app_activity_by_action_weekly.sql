

SELECT
    toStartOfWeek(date, 1)                                              AS week,
    activity_kind                                                       AS activity_kind,
    sum(n_events)                                                       AS n_events,
    countDistinct(address)                                              AS n_users,
    sum(amount_usd)                                                     AS amount_usd
FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
GROUP BY week, activity_kind
ORDER BY week, activity_kind