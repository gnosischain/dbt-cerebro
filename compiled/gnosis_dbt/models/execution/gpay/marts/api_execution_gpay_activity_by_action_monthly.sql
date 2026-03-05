

SELECT
    month,
    action,
    sum(activity_count)       AS activity_count,
    round(toFloat64(sum(volume_usd)), 2)  AS volume_usd,
    round(toFloat64(sum(volume)), 6)      AS volume_native
FROM `dbt`.`fct_execution_gpay_actions_by_token_monthly`
GROUP BY action, month
ORDER BY month, action