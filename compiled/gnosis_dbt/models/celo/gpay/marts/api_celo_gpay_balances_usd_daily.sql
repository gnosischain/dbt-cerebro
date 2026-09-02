

SELECT
    date,
    symbol      AS label,
    balance_usd AS value
FROM `dbt`.`fct_celo_gpay_balances_by_token_daily`
WHERE symbol IN ('USDC', 'USDT')
  AND date < today()
ORDER BY date, label