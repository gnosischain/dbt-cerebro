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
        tags                    = ['production','contracts','seerpm','wrapped1155factory','calls'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
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
