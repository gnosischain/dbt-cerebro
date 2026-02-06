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
        tags                    = ['production','contracts','gbcdeposit','calls']
    )
}}


{{ 
    decode_calls(
        tx_table      = source('execution','transactions'),
        contract_address  = '0x0B98057eA310F4d31F2a452B414647007d1645d9',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-12-01'
    )
}}
