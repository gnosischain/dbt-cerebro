

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    users                    AS value
FROM `dbt`.`fct_celo_gpay_retention_monthly`
ORDER BY date, label