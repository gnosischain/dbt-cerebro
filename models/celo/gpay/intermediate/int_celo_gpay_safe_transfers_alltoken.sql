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

{% set gp_start = var('celo_gp_start_date') %}

-- Deterministic all-token ERC-20 Transfer activity touching a GP card Safe on
-- EITHER side (int_celo_gpay_safe_registry), with NO token whitelist and NO
-- counterparty labeling. This is the base for the deterministic Tier-1
-- enrichment (all-token balances, per-counterparty flows, funding-relationship)
-- — everything downstream stays strictly within what is attributable to the
-- card Safe itself; nothing here infers who the counterparty "is".
--
-- Honesty about scale: `amount` (human units) is populated ONLY for tokens whose
-- decimals we actually know (celo_tokens_whitelist). For every other token we
-- keep amount_raw (the integer word) and leave amount/token_symbol NULL rather
-- than guessing a decimal scale. No USD here — pricing arbitrary tokens would
-- need a price source we don't have; USD stays in the whitelisted-token models.
--
-- A Safe-to-Safe transfer legitimately produces TWO rows (an 'out' row for the
-- sending Safe and an 'in' row for the receiving Safe) — each Safe's own
-- footprint should see its side. Do not dedupe across sides.
--
-- Cost note: this scans ALL Celo Transfer logs (no cheap token pre-filter is
-- possible for "all tokens") and keeps only rows where a registry Safe is on
-- one side. Heavy on a full-refresh, bounded per-month under incremental —
-- same insert_overwrite pattern as int_celo_gpay_transfers_native. While the
-- celo_execution backfill is still filling old months out of order, run with
-- --full-refresh (see int_celo_gpay_transfers_native header for the rationale).

WITH registry AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM {{ ref('int_celo_gpay_safe_registry') }}
),

whitelist AS (
    SELECT
        lower(replaceAll(address, '0x', '')) AS token_addr,
        symbol,
        decimals
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
    u.amount_raw,
    if(w.decimals IS NULL, NULL, toFloat64(u.amount_raw) / pow(10, w.decimals)) AS amount
FROM unioned u
LEFT JOIN whitelist w ON u.token_addr = w.token_addr
