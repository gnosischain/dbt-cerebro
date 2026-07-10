

SELECT
    date,
    symbol  AS label,
    balance AS value
FROM `dbt`.`fct_celo_gpay_balances_by_token_daily`
WHERE symbol IN ('USDC', 'USDT')
ORDER BY date, label