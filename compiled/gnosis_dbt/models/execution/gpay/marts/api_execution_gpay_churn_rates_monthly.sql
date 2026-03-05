

SELECT
    scope,
    toString(month) AS month,
    churn_rate,
    retention_rate
FROM `dbt`.`fct_execution_gpay_churn_monthly`
ORDER BY scope, month