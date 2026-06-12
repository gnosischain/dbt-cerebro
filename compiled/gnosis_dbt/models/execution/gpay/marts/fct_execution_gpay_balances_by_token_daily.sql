

-- Sourced from the user-holdings view (not the raw per-Safe balances) so
-- migrated old/new Safe pairs are not double counted once the refund of
-- the June 2026 exploit recovery lands on the new Safe.
SELECT
    date,
    symbol,
    sum(balance)                          AS balance,
    round(toFloat64(sum(balance_usd)), 2) AS balance_usd
FROM `dbt`.`int_execution_gpay_balances_user_daily`
GROUP BY date, symbol
ORDER BY date, symbol