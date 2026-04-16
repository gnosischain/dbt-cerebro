{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(contract_address, block_timestamp, transaction_hash, log_index)',
        unique_key              = '(contract_address, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1",
                                    "SET max_block_size = 5000"
                                ],
        tags                    = ['production','contracts','uniswapv3','events']
    )
}}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'UniswapV3Pool',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2022-04-22'  
    )
}}

