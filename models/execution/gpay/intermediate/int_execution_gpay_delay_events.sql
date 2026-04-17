{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(delay_module_address, block_timestamp, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(transaction_hash, log_index)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET allow_experimental_json_type = 1",
      "SET join_algorithm = 'grace_hash'"
    ],
    tags=['production','execution','gpay']
  )
}}

WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table         = source('execution','logs'),
            contract_address_ref = ref('contracts_gpay_modules_registry'),
            contract_type_filter = 'DelayModule',
            output_json_type     = true,
            incremental_column   = 'block_timestamp',
            start_blocktime      = '2023-06-01'
        ) }}
    )
)

SELECT
    concat('0x', lower(contract_address))                                  AS delay_module_address,
    event_name,
    decoded_params['queueNonce']                                           AS queue_nonce,
    decoded_params['txHash']                                               AS queued_tx_hash,
    decoded_params['to']                                                   AS inner_to,
    decoded_params['value']                                                AS inner_value,
    toUInt8OrNull(decoded_params['operation'])                             AS inner_operation,
    decoded_params['cooldown']                                             AS cooldown,
    decoded_params['expiration']                                           AS expiration,
    decoded_params['nonce']                                                AS nonce,
    block_timestamp,
    block_number,
    concat('0x', transaction_hash)                                         AS transaction_hash,
    log_index
FROM decoded
WHERE event_name IN (
    'DelaySetup','TransactionAdded','TxCooldownSet','TxExpirationSet','TxNonceSet'
)
