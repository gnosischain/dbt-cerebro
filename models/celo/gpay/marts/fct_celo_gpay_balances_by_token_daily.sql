{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, symbol)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay']
  )
}}

-- Total net-flow balance across all Celo GP card Safes, per day per token.
-- Mirrors fct_execution_gpay_balances_by_token_daily. Reads the dense
-- per-Safe base so the daily total is correct on every day (see that model's
-- header). Scope is the celo_tokens_whitelist tokens the base carries — there is
-- no filter here; USDT and USDC are simply the only ones that have ever had flow.
--
-- balance_usd is MARK-TO-MARKET (balance valued at that date's price), inherited
-- from the base — not a cost basis. It is NULL, never 0, for a token with no price
-- at or before the date, so sum() here skips it; the per-Safe base carries the
-- warn-severity tests that guard both the unpriced and the stale case.
-- token_class (STABLECOIN | RWA) is carried so consumers can separate spendable
-- float from reward holdings without hardcoding symbols.
SELECT
    date,
    token_symbol                          AS symbol,
    token_class,
    sum(balance)                          AS balance,
    round(toFloat64(sum(balance_usd)), 2) AS balance_usd
FROM {{ ref('fct_celo_gpay_balances_safe_daily') }}
GROUP BY date, symbol, token_class
ORDER BY date, symbol
