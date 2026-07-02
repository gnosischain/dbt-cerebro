{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy='append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(transaction_hash, log_index)',
        unique_key              = '(transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        tags                    = ['production','contracts','uniswapv3','events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0xae8fbe656a77519a7490054274910129c9244fa3',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2022-04-22'  
    )
}}
