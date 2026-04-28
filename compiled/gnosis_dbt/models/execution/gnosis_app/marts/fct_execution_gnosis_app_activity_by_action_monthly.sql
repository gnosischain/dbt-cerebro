

SELECT
    toStartOfMonth(date)                                                AS month,
    activity_kind                                                       AS activity_kind,
    sum(n_events)                                                       AS n_events,
    countDistinct(address)                                              AS n_users,
    sum(amount_usd)                                                     AS amount_usd
FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
WHERE toStartOfMonth(date) < toStartOfMonth(today())
GROUP BY month, activity_kind
ORDER BY month, activity_kind