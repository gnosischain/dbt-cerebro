

SELECT
    month      AS date,
    token      AS label,
    volume_usd AS value
FROM `dbt`.`fct_celo_gpay_actions_by_token_monthly`
WHERE action = 'Payment'
ORDER BY date, label