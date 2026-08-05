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
--
-- Unlike fct_celo_gpay_card_funding this genuinely needs SIGNED arithmetic (a
-- balance is inflow minus outflow), so it cannot simply cast unsigned. Instead the
-- cast is guarded: amount_raw is uint256 and Int256 tops out at 2^255-1, so a token
-- minting above that would sign-flip and an INFLOW would register as a huge
-- OUTFLOW. Over-range amounts become NULL — "not representable" rather than a
-- fabricated zero. sum() skips NULL, so such a transfer is excluded from the
-- running balance rather than corrupting it; the loud part is the warn test on
-- int_celo_gpay_safe_transfers_alltoken, which fires if any amount ever exceeds the
-- bound. Dormant today (max amount_raw 3.0e9 vs a 5.8e76 threshold, zero rows over
-- it) but two spoof tokens already reach these cards and a spoof token chooses its
-- own mint amount. Same defect class as EXECUTIONTRANSFERS-C03.

{% set int256_max = '57896044618658097711785492504343953926634992332820282019728792003956564819967' %}

WITH flows AS (
    SELECT
        block_date                                                    AS date,
        safe_address,
        token_address,
        token_symbol,
        if(amount_raw > toUInt256('{{ int256_max }}'),
           NULL,
           if(direction = 'in', toInt256(amount_raw), -toInt256(amount_raw))) AS signed_raw,
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
