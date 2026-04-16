{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET allow_experimental_json_type = 1",
      "SET join_algorithm = 'grace_hash'"
    ],
    tags=['production','execution','safe']
  )
}}

WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_safe_registry'),
            contract_type_filter = 'SafeProxy',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2020-05-21'
        ) }}
    )
    WHERE event_name IN ('EnabledModule','DisabledModule','ChangedGuard','ChangedModuleGuard')
)

SELECT
    concat('0x', lower(contract_address))               AS safe_address,
    multiIf(
        event_name = 'EnabledModule',     'enabled_module',
        event_name = 'DisabledModule',    'disabled_module',
        event_name = 'ChangedGuard',      'changed_guard',
                                          'changed_module_guard'
    )                                                   AS event_kind,
    lower(coalesce(
        nullIf(decoded_params['module'],      ''),
        nullIf(decoded_params['guard'],       ''),
        nullIf(decoded_params['moduleGuard'], '')
    ))                                                  AS target_address,
    block_timestamp,
    block_number,
    concat('0x', transaction_hash)                      AS transaction_hash,
    log_index
FROM decoded
