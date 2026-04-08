{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
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
        tags                    = ['production','contracts','cowprotocol','calls']
    )
}}

{{ 
    decode_calls(
        tx_table          = source('execution','transactions'),
        contract_address  = '0xbA3cB449bD2B4ADddBc894D8697F5170800EAdeC',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-01-01'
    )
}}
