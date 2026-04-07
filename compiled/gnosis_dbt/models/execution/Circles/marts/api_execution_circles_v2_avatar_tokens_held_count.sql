

-- Per-avatar count of distinct CRC tokens currently held with a
-- balance above the 0.001 CRC dust threshold (1e15 raw wei).
-- Backs the "Tokens Held" KPI card on the Circles Avatar tab.

WITH latest AS (
    SELECT max(date) AS d
    FROM `dbt`.`int_execution_circles_v2_balances_daily`
    WHERE date < today()
)
SELECT
    b.account                       AS avatar,
    uniqExact(b.token_address)      AS tokens_held_count
FROM `dbt`.`int_execution_circles_v2_balances_daily` b
CROSS JOIN latest
WHERE b.date = latest.d
  AND b.balance_raw > pow(10, 15)
GROUP BY b.account