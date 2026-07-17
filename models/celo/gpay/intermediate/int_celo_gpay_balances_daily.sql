{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, safe_address, token_symbol)',
    partition_by='toStartOfMonth(date)',
    tags=['production','execution','celo','gpay','balances_daily']
  )
}}

-- Cumulative net flow per Safe/token, not a real balance snapshot: Celo GP
-- Safes are all born post-launch (2026-06-23) so there is no opening-balance
-- problem, and there is no balance-snapshot source table for Celo in
-- ClickHouse to join against (unlike int_execution_tokens_balances_daily on
-- Gnosis Chain) — net-flow-since-inception is the correct and sufficient
-- balance signal. Rows only exist on days with flow activity; a line chart
-- reading this still renders continuously since it's a running total.

-- Inflows: Top-up (user-funded), Reversal (processor refund of a
-- failed/disputed charge) and Cashback (reward disbursed into the Safe) all add
-- to the float. Outflows: Payment and Withdrawal both remove from it. Cashback
-- is listed for forward-compatibility — it produces no rows until the cashback
-- disburser is configured (var celo_gp_cashback_sources), but listing it here
-- ensures a future reward inflow is added, not subtracted as a default outflow.
WITH daily_net AS (
    SELECT
        date,
        safe_address,
        token_symbol,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal', 'Cashback') THEN amount ELSE -amount END)      AS net_amount,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal', 'Cashback') THEN amount_usd ELSE -amount_usd END) AS net_amount_usd
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    GROUP BY date, safe_address, token_symbol
)

SELECT
    date,
    safe_address,
    token_symbol,
    SUM(net_amount)     OVER (PARTITION BY safe_address, token_symbol ORDER BY date) AS balance,
    SUM(net_amount_usd) OVER (PARTITION BY safe_address, token_symbol ORDER BY date) AS balance_usd
FROM daily_net
