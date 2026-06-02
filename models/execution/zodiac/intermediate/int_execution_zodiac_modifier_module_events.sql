{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='(modifier_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','zodiac', 'microbatch'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET allow_experimental_json_type = 0", "SET join_algorithm = 'default'"]
  )
}}
WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_zodiac_modules_registry'),
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2023-11-01'
        ) }}
    )
    WHERE event_name IN ('EnabledModule','DisabledModule')
)

SELECT
    concat('0x', lower(contract_address))               AS modifier_address,
    multiIf(
        event_name = 'EnabledModule',  'enabled_module',
                                       'disabled_module'
    )                                                   AS event_kind,
    lower(nullIf(decoded_params['module'], ''))         AS submodule_address,
    block_timestamp,
    block_number,
    concat('0x', transaction_hash)                      AS transaction_hash,
    log_index
FROM decoded
