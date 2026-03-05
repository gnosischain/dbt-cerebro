

SELECT round(toFloat64(balance), 2) AS value
FROM `dbt`.`fct_execution_gpay_balances_by_token_daily`
WHERE symbol = 'GNO'
ORDER BY date DESC
LIMIT 1