{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_timestamp, log_index)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','safe'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET enable_analyzer = 0"],
    post_hook=["SET allow_experimental_json_type = 0", "SET enable_analyzer = 1"]
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
-- materialized='table' (full rebuild), NOT incremental, deliberately: the
-- celo_execution backfill is still in progress, so rows for OLD months keep
-- appearing — a block_number watermark or current-month insert_overwrite
-- would silently skip them. Output is bounded by card count. Flip to the
-- Gnosis-style microbatch append once the indexer follows head.

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
    -- Pre-filter before ARRAY JOIN (same NULL-range() guard as the Gnosis
    -- int_execution_safes_owner_events model).
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        'safe_setup'                                                AS event_kind,
        lower(JSONExtractString(d.decoded_params['owners'], idx))   AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])               AS threshold,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SafeSetup') d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['owners'])) + 1) AS idx
),

owner_delta_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        if(d.event_name = 'AddedOwner', 'added_owner', 'removed_owner') AS event_kind,
        lower(d.decoded_params['owner'])                            AS owner,
        CAST(NULL AS Nullable(UInt32))                              AS threshold,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index
    FROM decoded d
    WHERE d.event_name IN ('AddedOwner','RemovedOwner')
)

SELECT * FROM safe_setup_rows
UNION ALL
SELECT * FROM owner_delta_rows
