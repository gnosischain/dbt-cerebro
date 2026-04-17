{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index, owner)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET allow_experimental_json_type = 1",
      "SET join_algorithm = 'grace_hash'"
    ],
    tags=['production','execution','safe']
  )
}}

WITH decoded AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        lower(contract_address) AS safe_address,
        event_name,
        decoded_params
    FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_safe_registry'),
            contract_type_filter = 'SafeProxy',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2020-05-21'
        ) }}
    )
),

safe_setup_rows AS (
    -- Pre-filter to SafeSetup rows BEFORE the ARRAY JOIN. ClickHouse
    -- evaluates ARRAY JOIN's range() before the outer WHERE, so without
    -- the subquery it tries to compute range(1, JSONLength(NULL) + 1)
    -- on every non-SafeSetup row and fails with `Illegal (null) value`.
    -- Same fix as in int_execution_gpay_roles_events.assign_role_rows.
    SELECT
        concat('0x',lower(d.safe_address))                     AS safe_address,
        'safe_setup'                                           AS event_kind,
        lower(JSONExtractString(d.decoded_params['owners'], idx))    AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])          AS threshold,
        d.block_timestamp,
        d.block_number,
        concat('0x',d.transaction_hash)                       AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SafeSetup') d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['owners'])) + 1) AS idx
),

owner_delta_rows AS (
    SELECT
        concat('0x',lower(d.safe_address))                     AS safe_address,
        if(d.event_name = 'AddedOwner', 'added_owner', 'removed_owner') AS event_kind,
        lower(d.decoded_params['owner'])                       AS owner,
        CAST(NULL AS Nullable(UInt32))                         AS threshold,
        d.block_timestamp,
        d.block_number,
        concat('0x',d.transaction_hash)                       AS transaction_hash,
        d.log_index
    FROM decoded d
    WHERE d.event_name IN ('AddedOwner','RemovedOwner')
),

threshold_change_rows AS (
    SELECT
        concat('0x',lower(d.safe_address))                     AS safe_address,
        'changed_threshold'                                    AS event_kind,
        CAST(NULL AS Nullable(String))                         AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])          AS threshold,
        d.block_timestamp,
        d.block_number,
        concat('0x',d.transaction_hash)                       AS transaction_hash,
        d.log_index
    FROM decoded d
    WHERE d.event_name = 'ChangedThreshold'
)

SELECT * FROM safe_setup_rows
UNION ALL
SELECT * FROM owner_delta_rows
UNION ALL
SELECT * FROM threshold_change_rows
