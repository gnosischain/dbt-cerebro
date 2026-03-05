

SELECT
    week        AS date,
    token       AS label,
    volume_usd  AS value
FROM `dbt`.`fct_execution_gpay_actions_by_token_weekly`
WHERE action = 'Payment'
ORDER BY date, label