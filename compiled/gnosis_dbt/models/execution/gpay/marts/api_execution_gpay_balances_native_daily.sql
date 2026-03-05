

SELECT
    date,
    symbol  AS label,
    balance AS value
FROM `dbt`.`fct_execution_gpay_balances_by_token_daily`
WHERE symbol IN ('EURe', 'GBPe', 'USDC.e', 'GNO')
ORDER BY date, label