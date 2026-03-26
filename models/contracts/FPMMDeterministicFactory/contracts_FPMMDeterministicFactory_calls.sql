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
        tags                    = ['production','contracts','fpmm','calls']
    )
}}


{{ 
    decode_calls(
        tx_table      = source('execution','transactions'),
        contract_address  = '0x9083a2b699c0a4ad06f63580bde2635d26a3eef0',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2020-09-04'
    )
}}
