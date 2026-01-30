{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash, log_index)',
        unique_key              = '(transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['dev','contracts','uniswapv3','events']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0xe32f7dd7e3f098d518ff19a22d5f028e076489b1',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2022-04-22'  
    )
}}

