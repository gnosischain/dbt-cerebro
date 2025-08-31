{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['production','aave','v3','contracts','events']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x7304979ec9E4EaA0273b6A037a31c4e9e5A75D16',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-10-04'
    )
}}
