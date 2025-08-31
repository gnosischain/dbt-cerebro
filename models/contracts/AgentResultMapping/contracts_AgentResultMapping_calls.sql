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
        tags                    = ['production','contracts','agentresultmapping','calls']
    )
}}



{{ 
    decode_calls(
        tx_table      = source('execution','transactions'),
        contract_address  = '0x99c43743a2dbd406160cc43cf08113b17178789c',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2025-06-30'
    )
}}
