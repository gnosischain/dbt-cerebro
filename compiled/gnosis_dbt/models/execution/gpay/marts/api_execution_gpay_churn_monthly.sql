

SELECT
    scope,
    toString(month) AS month,
    new_users,
    retained_users,
    returning_users,
    churned_users,
    total_active,
    churn_rate,
    retention_rate
FROM `dbt`.`fct_execution_gpay_churn_monthly`
ORDER BY scope, month