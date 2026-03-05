

SELECT 'native' AS unit, week AS date, volume AS value
FROM `dbt`.`fct_execution_gpay_actions_by_token_weekly`
WHERE action = 'Cashback'

UNION ALL

SELECT 'usd' AS unit, week AS date, volume_usd AS value
FROM  `dbt`.`fct_execution_gpay_actions_by_token_weekly`
WHERE action = 'Cashback'

ORDER BY date