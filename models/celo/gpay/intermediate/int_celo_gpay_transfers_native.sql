{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(block_time, tx_hash, log_index)',
    partition_by='toStartOfMonth(block_date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','transfers']
  )
}}

-- Native twin of crawlers_data.celo_gpay_transfers: whitelisted-token ERC-20
-- Transfer events touching a GP card Safe, decoded straight from
-- celo_execution.logs. Same column shape as the Dune-fed table (sender /
-- receiver / amount / amount_usd) so int_celo_gpay_activity can be repointed
-- with a ref swap after reconciliation.
--
-- Transfer is raw-sliced rather than run through decode_logs: the layout is
-- fixed (from=topic1, to=topic2, value=data word 0), the token set is 4
-- addresses, and skipping the ABI join keeps this — the highest-volume scan
-- in the Celo pipeline — cheap.
--
-- amount_usd prices at the transfer's DATE via the Celo price hub. XAUt0 has
-- no feed yet (no Chainlink XAU on Celo, not in the Dune price dump) so its
-- amount_usd is NULL, not 0 — visibly unpriced rather than silently zero.
--
-- materialized='incremental' (insert_overwrite + apply_monthly_incremental_filter),
-- the prod shape — mirrors int_celo_gpay_activity. This is the only native model
-- that scales with tx volume (it scans all of celo_execution.logs), so it's the
-- one that benefits from incrementality; the registry / wallet / safe-event
-- models stay full-rebuild tables (bounded by card count, and the registries are
-- min() aggregations that cannot be incremental).
-- IMPORTANT while the celo_execution backfill is still in flight: run this with
-- --full-refresh. A plain incremental run only recomputes the current month's
-- partition, so any OLDER month the backfill fills in later (out-of-order
-- arrival) would be missed. Once the indexer follows head (data only arrives at
-- the tip), plain daily incremental runs are correct and cheap.

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
          AND lower(replaceAll(address, '0x', '')) IN (SELECT token_addr FROM whitelist)
          AND block_timestamp >= toDateTime('2026-01-01')
          -- On incremental runs, prune the log scan to the current month(s);
          -- emits nothing under --full-refresh (full rebuild). block_date is
          -- this model's own partition/date column.
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_date', true) }}
    )
    WHERE _dedup_rn = 1
),

decoded AS (
    SELECT
        toDate(l.block_timestamp)                                            AS block_date,
        l.block_timestamp                                                    AS block_time,
        concat('0x', lower(replaceAll(l.transaction_hash, '0x', '')))        AS tx_hash,
        l.log_index,
        substring(replaceAll(l.topic1, '0x', ''), 25, 40)                    AS sender_raw,
        substring(replaceAll(l.topic2, '0x', ''), 25, 40)                    AS receiver_raw,
        lower(replaceAll(l.address, '0x', ''))                               AS token_addr,
        reinterpretAsUInt256(reverse(unhex(
            substring(replaceAll(l.data, '0x', ''), 1, 64)
        )))                                                                  AS value_raw
    FROM transfer_logs l
)

SELECT
    d.block_date,
    d.block_time,
    d.tx_hash,
    d.log_index,
    concat('0x', lower(d.sender_raw))     AS sender,
    concat('0x', lower(d.receiver_raw))   AS receiver,
    concat('0x', w.token_addr)            AS token_address,
    w.symbol                              AS token_symbol,
    toFloat64(d.value_raw) / pow(10, w.decimals)                    AS amount,
    (toFloat64(d.value_raw) / pow(10, w.decimals)) * nullIf(p.price, 0) AS amount_usd
FROM decoded d
INNER JOIN whitelist w
    ON d.token_addr = w.token_addr
LEFT JOIN {{ ref('int_celo_token_prices_daily') }} p
    ON p.date = d.block_date
   AND p.symbol = w.symbol
WHERE lower(d.sender_raw)   IN (SELECT addr FROM registry)
   OR lower(d.receiver_raw) IN (SELECT addr FROM registry)
