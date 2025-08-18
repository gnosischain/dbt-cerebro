{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_number, log_index)',
        unique_key='(block_number, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={
            'allow_nullable_key': 1
        }
    ) 
}}


SELECT
    block_number
    ,block_timestamp
    ,transaction_index
    ,log_index
    ,transaction_hash
    ,token_address
    ,"from"
    ,"to"
    ,"value"
FROM {{ ref('int_transfers_erc20') }}
WHERE
    token_address = '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d'
    {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', 'true') }}


