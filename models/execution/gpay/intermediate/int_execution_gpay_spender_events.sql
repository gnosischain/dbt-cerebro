{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(spender_module_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET allow_experimental_json_type = 0", "SET join_algorithm = 'default'"]
  )
}}
WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_gpay_modules_registry'),
            contract_type_filter = 'SpenderModule',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2023-06-01'
        ) }}
    )
)

SELECT
    concat('0x', lower(contract_address))                                   AS spender_module_address,
    event_name,

    -- Spend payload (only populated when event_name='Spend')
    lower(decoded_params['asset'])                                          AS spend_asset,
    lower(decoded_params['account'])                                        AS spend_account,
    lower(decoded_params['receiver'])                                       AS spend_receiver,
    decoded_params['amount']                                                AS spend_amount,

    -- AvatarSet payload
    lower(decoded_params['previousAvatar'])                                 AS previous_avatar,
    lower(decoded_params['newAvatar'])                                      AS new_avatar,

    -- TargetSet payload
    lower(decoded_params['previousTarget'])                                 AS previous_target,
    lower(decoded_params['newTarget'])                                      AS new_target,

    -- Module-management payload (Enabled/Disabled/ExecutionFromModule*)
    lower(decoded_params['module'])                                         AS module_address,

    -- OwnershipTransferred payload
    lower(decoded_params['previousOwner'])                                  AS previous_owner,
    lower(decoded_params['newOwner'])                                       AS new_owner,

    -- Initialized payload
    decoded_params['version']                                               AS init_version,

    block_timestamp,
    block_number,
    concat('0x', transaction_hash)                                          AS transaction_hash,
    log_index
FROM decoded
WHERE event_name IN (
    'Spend',
    'AvatarSet',
    'TargetSet',
    'EnabledModule',
    'DisabledModule',
    'ExecutionFromModuleSuccess',
    'ExecutionFromModuleFailure',
    'OwnershipTransferred',
    'Initialized',
    'HashExecuted',
    'HashInvalidated'
)
