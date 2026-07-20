{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, safe_address, token_address)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','balances','alltoken']
  )
}}

-- Deterministic all-token running balance per card Safe, from
-- int_celo_gpay_safe_transfers_alltoken. Same net-flow-since-inception idiom as
-- fct_celo_gpay_balances_safe_daily (Celo Safes are born post-launch, no opening
-- balance, no snapshot source), but across EVERY token the Safe touches, not
-- just the whitelist.
--
-- Two balance columns, no guessing: balance_raw is the exact integer net flow
-- (always populated); balance is the human-unit version, populated ONLY for
-- tokens whose decimals are known (celo_tokens_whitelist) and NULL otherwise —
-- we do not invent a decimal scale for unknown tokens. No USD (no price source
-- for arbitrary tokens). Rows exist only on days with flow; a running total
-- still reads continuously on a chart.

WITH flows AS (
    SELECT
        block_date                                                    AS date,
        safe_address,
        token_address,
        token_symbol,
        if(direction = 'in', toInt256(amount_raw), -toInt256(amount_raw)) AS signed_raw,
        if(direction = 'in', amount, -amount)                         AS signed_amount
    FROM {{ ref('int_celo_gpay_safe_transfers_alltoken') }}
),

daily_net AS (
    SELECT
        date,
        safe_address,
        token_address,
        any(token_symbol)   AS token_symbol,
        sum(signed_raw)     AS net_raw,
        sum(signed_amount)  AS net_amount
    FROM flows
    GROUP BY date, safe_address, token_address
)

SELECT
    date,
    safe_address,
    token_address,
    token_symbol,
    sum(net_raw)    OVER (PARTITION BY safe_address, token_address ORDER BY date) AS balance_raw,
    sum(net_amount) OVER (PARTITION BY safe_address, token_address ORDER BY date) AS balance
FROM daily_net
