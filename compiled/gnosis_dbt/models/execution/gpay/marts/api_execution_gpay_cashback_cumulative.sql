

SELECT 'native' AS unit, week AS date, volume_cumulative AS value
FROM `dbt`.`fct_execution_gpay_actions_by_token_weekly`
WHERE action = 'Cashback'

UNION ALL

SELECT 'usd' AS unit, week AS date, volume_usd_cumulative AS value
FROM `dbt`.`fct_execution_gpay_actions_by_token_weekly`
WHERE action = 'Cashback'

ORDER BY date