




  

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
-- That NULL-for-unknown contract is why join_use_nulls = 1 is set in the pre_hook
-- (docs/lessons/ch-left-join-nulls.md). The whitelist seed's columns are non-nullable,
-- so at the ClickHouse default a non-whitelisted token leaves the LEFT JOIN as
-- symbol = '' and decimals = 0: `w.decimals IS NULL` never fires, `amount` silently
-- gets a 10^0 scale (raw integer read as human units), and int_celo_gpay_activity's
-- `token_symbol IS NOT NULL` whitelist gate matches every row instead of none.
--
-- A Safe-to-Safe transfer legitimately produces TWO rows (an 'out' row for the
-- sending Safe and an 'in' row for the receiving Safe) — each Safe's own
-- footprint should see its side. Do not dedupe across sides here
-- (int_celo_gpay_activity collapses to sender-side for its per-transfer grain).
--
-- Cost note: scans ALL Celo Transfer logs (no cheap token pre-filter for "all
-- tokens") and keeps only rows where a registry Safe is on one side. Heavy on a
-- full-refresh, bounded per-month under incremental. The celo_execution backfill
-- is complete and the indexer follows head, so plain daily incremental is the
-- correct path; a rebuild is only needed after a registry change that adds Safes
-- whose history predates the current partitions.
--
-- Watch the intra-month cost curve: the monthly insert_overwrite re-reads the
-- whole current month of all-chain Transfer logs on every run, so cost climbs
-- through the month and resets. Revisit the partition grain (or add a token
-- pre-filter) before Celo GP volume grows another order of magnitude.
--
-- incremental_strategy resolves to `append` when start_month is set: refresh.py
-- drives the staged monthly backfill, and a staged window narrower than the month
-- partition would make insert_overwrite's REPLACE PARTITION discard the rest of
-- that month (docs/lessons/staged-insert-overwrite-wipe.md).
-- The scoped-append path is a backfill-into-EMPTY-months tool only: re-running it
-- over a month that already holds rows appends a second copy, and marts read this
-- table without FINAL (docs/lessons/append-over-populated-duplicates.md). To
-- reprocess an existing month, drop its partition first (macros/db/drop_partition)
-- and then re-run scoped.

WITH registry AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM `dbt`.`int_celo_gpay_safe_registry`
),

whitelist AS (
    SELECT
        lower(replaceAll(address, '0x', '')) AS token_addr,
        symbol,
        decimals,
        token_class
    FROM `dbt`.`celo_tokens_whitelist`
),

transfer_logs AS (
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY block_number, transaction_index, log_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM `celo_execution`.`logs`
        WHERE replaceAll(topic0, '0x', '') = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
          AND block_timestamp >= toDateTime('2026-01-01')
          -- Full-refresh batching: scripts/full_refresh/refresh.py passes
          -- start_month/end_month per monthly batch (see meta.full_refresh) so
          -- this all-Transfer scan is bounded to one month at a time and never
          -- materialises the whole logs table in a single query (OOM).
          
          
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_date)), -0))
        FROM `dbt`.`int_celo_gpay_safe_transfers_alltoken` AS x1
        WHERE 1=1 
      )
      
    
  

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

-- Every projected column carries an explicit alias, including the pass-throughs.
-- Under the legacy analyzer (enable_analyzer = 0) a bare `u.block_date` is named
-- `u.block_date` in the result header, so PARTITION BY toStartOfMonth(block_date)
-- and the order_by keys cannot resolve at CREATE TABLE time (CH code 47). The
-- aliases make the header identical under either analyzer.
SELECT
    u.block_date                                                      AS block_date,
    u.block_time                                                      AS block_time,
    u.tx_hash                                                         AS tx_hash,
    u.log_index                                                       AS log_index,
    u.safe_address                                                    AS safe_address,
    u.direction                                                       AS direction,
    u.counterparty                                                    AS counterparty,
    concat('0x', u.token_addr)                                        AS token_address,
    w.symbol                                                          AS token_symbol,
    w.token_class                                                     AS token_class,
    u.amount_raw                                                      AS amount_raw,
    if(w.decimals IS NULL, NULL, toFloat64(u.amount_raw) / pow(10, w.decimals)) AS amount,
    if(w.decimals IS NULL, NULL,
       (toFloat64(u.amount_raw) / pow(10, w.decimals)) * nullIf(p.price, 0))    AS amount_usd
FROM unioned u
LEFT JOIN whitelist w ON u.token_addr = w.token_addr
LEFT JOIN `dbt`.`int_celo_token_prices_daily` p
    ON p.date = u.block_date AND p.symbol = w.symbol