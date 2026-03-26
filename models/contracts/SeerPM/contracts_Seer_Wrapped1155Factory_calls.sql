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
        tags                    = ['production','contracts','seerpm','wrapped1155factory','calls']
    )
}}


{{ 
    decode_calls(
        tx_table      = source('execution','transactions'),
        contract_address  = '0xd194319d1804c1051dd21ba1dc931ca72410b79f',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2024-02-07'
    )
}}
