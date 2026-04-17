

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    activity_kind,
    retention_pct,
    users                    AS value_abs,
    initial_users
FROM `dbt`.`fct_execution_gnosis_app_retention_by_action_monthly`
ORDER BY activity_kind, y, x