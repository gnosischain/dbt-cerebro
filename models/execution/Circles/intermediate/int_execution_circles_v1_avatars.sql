{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, user_address)',
        unique_key              = '(block_timestamp, user_address)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        tags                    = ['dev','execution','circles','avatars']
    )
}}


SELECT
    block_timestamp
    ,decoded_params['avatar'] AS user_address
    ,decoded_params['inviter'] AS inviter_address
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE 
    event_name = 'RegisterHuman'
    {{ apply_monthly_incremental_filter(source_field='block_timestamp',destination_field='date',add_and=false) }}