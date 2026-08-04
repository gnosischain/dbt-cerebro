{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index, owner)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','safe','microbatch'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET enable_analyzer = 0"],
    post_hook=["SET allow_experimental_json_type = 0", "SET enable_analyzer = 1"],
    query_settings={
        'max_threads': '1',
        'max_memory_usage': '4000000000',
        'memory_usage_overcommit_max_wait_microseconds': '60000000',
        'max_bytes_before_external_group_by': '20000000',
        'max_bytes_before_external_sort':     '20000000',
        'use_uncompressed_cache': '0',
        'use_query_cache': '0'
    }
  )
}}
-- Safe lifecycle events for GP card Safes on Celo, decoded through the
-- multichain decode pipeline (chain='celo'): SafeSetup (initial owners),
-- AddedOwner, RemovedOwner. The ABI is resolved from the GnosisSafe v1.3.0 L2
-- singleton row in the celo partition of the signature seeds
-- (abi_source_address override — the events live on the proxies, the ABI on
-- the singleton; SafeSetup/AddedOwner/RemovedOwner topic0s are identical
-- across Safe 1.3.0/1.4.1 so one ABI source covers both).
--
-- Incremental append + monthly partitions, mirroring Gnosis Chain's
-- int_execution_safes_owner_events. The output is tiny (bounded by card count)
-- but the COST is the scan of celo_execution.logs, which a full rebuild re-reads
-- over the whole GP era on every run — that is what OOMed once the Celo backfill
-- landed (CH code 241, OvercommitTracker victim). Append + the decode_logs
-- block_number watermark means a daily run reads only new blocks, and a rebuild
-- goes through scripts/full_refresh/refresh.py in the monthly batches declared
-- in meta.full_refresh (decode_logs honors start_month/end_month).
--
-- Consequence of the watermark, same as every other append decode stream here
-- (contracts_celo_chainlink_feeds_events documents the identical hazard): logs
-- landing in celo_execution BELOW the high-water mark are never decoded. The
-- backfill is complete, so the remaining trigger is registry growth: whenever
-- int_celo_gpay_safe_registry discovers a Safe whose SafeSetup predates the
-- watermark, the affected months must be re-decoded explicitly — drop those
-- partitions first (macros/db/drop_partition.sql), because appending over a
-- populated month duplicates rows and int_celo_gpay_wallet_events reads this
-- table without FINAL (docs/lessons/append-over-populated-duplicates.md).
--
-- Every projected column is explicitly aliased: enable_analyzer = 0 (needed for
-- decode planning speed) names a bare `d.block_timestamp` as `d.block_timestamp`
-- in the result header, which the partition key and order_by could not resolve.

WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('celo_execution','logs'),
            contract_address_ref = ref('int_celo_gpay_safe_registry'),
            contract_type_filter = 'SafeProxy',
            abi_source_address   = '0x3e5c63644e683549055b9be8653de26e0b4cd36e',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2026-01-01',
            event_name_filter    = ['SafeSetup','AddedOwner','RemovedOwner'],
            chain                = 'celo'
        ) }}
    )
    WHERE event_name IN ('SafeSetup','AddedOwner','RemovedOwner')
),

safe_setup_rows AS (
    -- One row per decoded owner. Guard: only setups whose owners array
    -- actually decoded (length > 0) reach the ARRAY JOIN — an empty array
    -- makes range(1,1) empty and the row would silently drop. Owner-less
    -- setups are re-added by safe_setup_ownerless_rows below.
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        'safe_setup'                                                AS event_kind,
        lower(JSONExtractString(d.decoded_params['owners'], idx))   AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])               AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SafeSetup' AND JSONLength(decoded_params['owners']) > 0) d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['owners'])) + 1) AS idx
),

safe_setup_ownerless_rows AS (
    -- Parity with the Dune spine: a SafeSetup whose owners array failed to
    -- decode still ISSUES the card, with a NULL owner, instead of vanishing
    -- from the wallet list entirely (observed at least once on Celo — a
    -- setup that needed a raw-byte owner fallback). action_value is Nullable
    -- downstream, so NULL is the honest "owner unknown" signal, not a
    -- silently dropped card. Keeps native issuance counts matching Dune.
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        'safe_setup'                                                AS event_kind,
        CAST(NULL AS Nullable(String))                              AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])               AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM decoded d
    WHERE d.event_name = 'SafeSetup'
      AND coalesce(JSONLength(d.decoded_params['owners']), 0) = 0
),

owner_delta_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        if(d.event_name = 'AddedOwner', 'added_owner', 'removed_owner') AS event_kind,
        lower(d.decoded_params['owner'])                            AS owner,
        CAST(NULL AS Nullable(UInt32))                              AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM decoded d
    WHERE d.event_name IN ('AddedOwner','RemovedOwner')
)

SELECT * FROM safe_setup_rows
UNION ALL
SELECT * FROM safe_setup_ownerless_rows
UNION ALL
SELECT * FROM owner_delta_rows
