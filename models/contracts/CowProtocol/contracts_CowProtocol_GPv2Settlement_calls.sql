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
                                    "SET allow_experimental_json_type = 1",
                                    "SET max_block_size = 5000"
                                ],
        tags                    = ['production','contracts','cowprotocol','calls']
    )
}}

{{ 
    decode_calls(
        tx_table          = source('execution','transactions'),
        contract_address  = '0x9008D19f58AAbD9eD0D60971565AA8510560ab41',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-04-01'
    )
}}
