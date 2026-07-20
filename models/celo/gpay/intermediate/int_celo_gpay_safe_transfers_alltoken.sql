{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_time, tx_hash, log_index)',
    partition_by='toStartOfMonth(block_date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','transfers','alltoken']
  )
}}

{% set gp_start = '2026-01-01' %}  {# GP era floor #}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

-- Deterministic all-token ERC-20 Transfer activity touching a GP card Safe on
-- EITHER side (int_celo_gpay_safe_registry), with NO token whitelist and NO
-- counterparty labeling. This is the base for the deterministic Tier-1
-- enrichment (all-token balances, per-counterparty flows, funding-relationship)
-- — everything downstream stays strictly within what is attributable to the
-- card Safe itself; nothing here infers who the counterparty "is".
--
-- THE single transfer base for the Celo GP pipeline: int_celo_gpay_activity
-- classifies its whitelisted subset (payments/top-ups/etc.), and the card_*
-- marts use its full all-token footprint (holdings, funder fan-out).
--
-- Honesty about scale: `amount` (human units) + `amount_usd` are populated ONLY
-- for whitelisted tokens (celo_tokens_whitelist — address-gated, so spoof tokens
-- reusing "USDC"/"USD₮" symbols are excluded). For every other token we keep
-- amount_raw (the integer word) and leave amount/amount_usd/token_symbol/
-- token_class NULL rather than guessing. amount_usd prices at the transfer date
-- via the Celo price hub; NULL when unpriced (visibly unpriced, never 0).
--
-- A Safe-to-Safe transfer legitimately produces TWO rows (an 'out' row for the
-- sending Safe and an 'in' row for the receiving Safe) — each Safe's own
-- footprint should see its side. Do not dedupe across sides here
-- (int_celo_gpay_activity collapses to sender-side for its per-transfer grain).
--
-- Cost note: scans ALL Celo Transfer logs (no cheap token pre-filter for "all
-- tokens") and keeps only rows where a registry Safe is on one side. Heavy on a
-- full-refresh, bounded per-month under incremental. While the celo_execution
-- backfill is still filling old months out of order, run with --full-refresh;
-- plain daily incremental is correct once the indexer follows head.

WITH registry AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM {{ ref('int_celo_gpay_safe_registry') }}
),

whitelist AS (
    SELECT
        lower(replaceAll(address, '0x', '')) AS token_addr,
        symbol,
        decimals,
        token_class
    FROM {{ ref('celo_tokens_whitelist') }}
),

transfer_logs AS (
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY block_number, transaction_index, log_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM {{ source('celo_execution', 'logs') }}
        WHERE replaceAll(topic0, '0x', '') = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
          AND block_timestamp >= toDateTime('{{ gp_start }}')
          -- Full-refresh batching: scripts/full_refresh/refresh.py passes
          -- start_month/end_month per monthly batch (see meta.full_refresh) so
          -- this all-Transfer scan is bounded to one month at a time and never
          -- materialises the whole logs table in a single query (OOM).
          {% if start_month is not none and end_month is not none %}
          AND block_timestamp >= toDateTime('{{ start_month }}')
          AND block_timestamp <  toDateTime('{{ end_month }}') + INTERVAL 1 MONTH
          {% endif %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_date', true) }}
    )
    WHERE _dedup_rn = 1
),

decoded AS (
    SELECT
        toDate(l.block_timestamp)                                     AS block_date,
        l.block_timestamp                                            AS block_time,
        concat('0x', lower(replaceAll(l.transaction_hash, '0x', ''))) AS tx_hash,
        l.log_index,
        substring(replaceAll(l.topic1, '0x', ''), 25, 40)            AS from_raw,
        substring(replaceAll(l.topic2, '0x', ''), 25, 40)            AS to_raw,
        lower(replaceAll(l.address, '0x', ''))                       AS token_addr,
        reinterpretAsUInt256(reverse(unhex(
            substring(replaceAll(l.data, '0x', ''), 1, 64)
        )))                                                          AS amount_raw
    FROM transfer_logs l
    WHERE substring(replaceAll(l.topic1, '0x', ''), 25, 40) IN (SELECT addr FROM registry)
       OR substring(replaceAll(l.topic2, '0x', ''), 25, 40) IN (SELECT addr FROM registry)
),

-- One row per (transfer, Safe-side): the Safe as sender (out) and/or receiver (in).
outbound AS (
    SELECT
        block_date, block_time, tx_hash, log_index,
        concat('0x', from_raw)  AS safe_address,
        'out'                   AS direction,
        concat('0x', to_raw)    AS counterparty,
        token_addr, amount_raw
    FROM decoded
    WHERE from_raw IN (SELECT addr FROM registry)
),

inbound AS (
    SELECT
        block_date, block_time, tx_hash, log_index,
        concat('0x', to_raw)    AS safe_address,
        'in'                    AS direction,
        concat('0x', from_raw)  AS counterparty,
        token_addr, amount_raw
    FROM decoded
    WHERE to_raw IN (SELECT addr FROM registry)
),

unioned AS (
    SELECT * FROM outbound
    UNION ALL
    SELECT * FROM inbound
)

SELECT
    u.block_date,
    u.block_time,
    u.tx_hash,
    u.log_index,
    u.safe_address,
    u.direction,
    u.counterparty,
    concat('0x', u.token_addr)                                        AS token_address,
    w.symbol                                                          AS token_symbol,
    w.token_class                                                     AS token_class,
    u.amount_raw,
    if(w.decimals IS NULL, NULL, toFloat64(u.amount_raw) / pow(10, w.decimals)) AS amount,
    if(w.decimals IS NULL, NULL,
       (toFloat64(u.amount_raw) / pow(10, w.decimals)) * nullIf(p.price, 0))    AS amount_usd
FROM unioned u
LEFT JOIN whitelist w ON u.token_addr = w.token_addr
LEFT JOIN {{ ref('int_celo_token_prices_daily') }} p
    ON p.date = u.block_date AND p.symbol = w.symbol
