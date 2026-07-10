

-- Total net-flow balance across all Celo GP card Safes, per day per token.
-- Mirrors fct_execution_gpay_balances_by_token_daily. Reads the dense
-- per-Safe base so the daily total is correct on every day (see that model's
-- header). USDC / USDT only.
SELECT
    date,
    token_symbol                          AS symbol,
    sum(balance)                          AS balance,
    round(toFloat64(sum(balance_usd)), 2) AS balance_usd
FROM `dbt`.`fct_celo_gpay_balances_safe_daily`
GROUP BY date, symbol
ORDER BY date, symbol