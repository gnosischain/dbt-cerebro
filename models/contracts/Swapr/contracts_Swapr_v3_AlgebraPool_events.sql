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
                                ],
        tags                    = ['production','contracts','swapr','events']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x2de7439f52d059e6cadbbeb4527683a94331cf65',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-11-30'
    )
}}
