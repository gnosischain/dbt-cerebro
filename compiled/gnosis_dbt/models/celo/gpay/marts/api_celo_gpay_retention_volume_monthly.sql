

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    amount_usd               AS value
FROM `dbt`.`fct_celo_gpay_retention_monthly`
ORDER BY date, label