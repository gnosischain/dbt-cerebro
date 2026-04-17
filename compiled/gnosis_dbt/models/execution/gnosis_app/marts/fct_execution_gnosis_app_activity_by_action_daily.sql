

SELECT
    date                                                                AS date,
    activity_kind                                                       AS activity_kind,
    sum(n_events)                                                       AS n_events,
    countDistinct(address)                                              AS n_users,
    sum(amount_usd)                                                     AS amount_usd
FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
GROUP BY date, activity_kind
ORDER BY date, activity_kind