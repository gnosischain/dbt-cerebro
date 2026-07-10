

SELECT
    action,
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    users                    AS value
FROM `dbt`.`fct_celo_gpay_retention_by_action_monthly`
ORDER BY action, date, label