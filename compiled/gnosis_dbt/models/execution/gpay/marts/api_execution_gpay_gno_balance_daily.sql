

SELECT
    date,
    balance AS value
FROM `dbt`.`fct_execution_gpay_balances_by_token_daily`
WHERE symbol = 'GNO'
ORDER BY date