{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash)',
        unique_key              = '(block_timestamp, transaction_hash)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ]
    )
}}


{{ 
    decode_calls(
        tx_table      = source('execution','transactions'),
        contract_address  = '0xa0864cca6e114013ab0e27cbd5b6f4c8947da766',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-09-22'
    )
}}
