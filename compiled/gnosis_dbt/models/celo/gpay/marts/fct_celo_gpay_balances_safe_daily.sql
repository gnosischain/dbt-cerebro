

-- Dense per-Safe/token/day net-flow balance. This is the Celo analog of
-- Gnosis Chain's int_execution_gpay_balances_user_daily, which is dense by
-- construction (it reads a chain-wide balance-diff snapshot). Celo has no
-- such snapshot source, so we DENSIFY here: a date spine x every
-- (safe, token) pair that has ever transacted, left-joined to daily net flow,
-- with the running total carried across the full range.
--
-- Why densify rather than reuse the sparse int_celo_gpay_balances_daily:
-- summing sparse per-Safe running totals by day undercounts, because on a
-- given day a Safe with no activity contributes no row (its carried-forward
-- balance would be missing from that day's SUM). Densifying first, then
-- aggregating downstream, makes both the daily balance time series AND the
-- latest-day snapshot correct (every Safe/token present on every day).
--
-- CORRECTNESS SCOPE: net flow == true on-chain balance holds ONLY for the two
-- whitelisted stablecoins (USDC, USDT), and only because (verified on-chain,
-- 2026-07) Celo GP Safes start at zero pre-launch, there are no Safe-to-Safe
-- transfers, and no other token carries real value. If a third settlement
-- token is ever added on Celo it must be added to the ingestion whitelist AND
-- surfaced here; until then balances intentionally cover USDC/USDT only.
-- Net-flow formula matches int_celo_gpay_balances_daily exactly (inflows =
-- Top-up + Reversal, outflows = Payment + Withdrawal).
WITH bounds AS (
    SELECT
        assumeNotNull(min(date)) AS min_date,
        assumeNotNull(max(date)) AS max_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
),

date_spine AS (
    SELECT toDate((SELECT min_date FROM bounds) + number) AS date
    FROM numbers(assumeNotNull(toUInt64((SELECT max_date FROM bounds) - (SELECT min_date FROM bounds) + 1)))
),

pairs AS (
    SELECT DISTINCT safe_address, token_symbol
    FROM `dbt`.`int_celo_gpay_activity_daily`
),

daily_net AS (
    SELECT
        date,
        safe_address,
        token_symbol,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal') THEN amount     ELSE -amount     END) AS net_amount,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal') THEN amount_usd ELSE -amount_usd END) AS net_amount_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    GROUP BY date, safe_address, token_symbol
),

grid AS (
    SELECT d.date, p.safe_address, p.token_symbol
    FROM date_spine d
    CROSS JOIN pairs p
)

SELECT
    g.date,
    g.safe_address,
    g.token_symbol,
    SUM(coalesce(n.net_amount, 0))     OVER (PARTITION BY g.safe_address, g.token_symbol ORDER BY g.date) AS balance,
    SUM(coalesce(n.net_amount_usd, 0)) OVER (PARTITION BY g.safe_address, g.token_symbol ORDER BY g.date) AS balance_usd
FROM grid g
LEFT JOIN daily_net n
    ON  n.date         = g.date
    AND n.safe_address = g.safe_address
    AND n.token_symbol = g.token_symbol