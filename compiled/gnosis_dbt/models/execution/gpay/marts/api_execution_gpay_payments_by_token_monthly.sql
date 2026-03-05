

SELECT
    month           AS date,
    token           AS label,
    activity_count  AS value
FROM `dbt`.`fct_execution_gpay_actions_by_token_monthly`
WHERE action = 'Payment'
ORDER BY date, label