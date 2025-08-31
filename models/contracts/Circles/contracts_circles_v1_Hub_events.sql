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
        tags                    = ['production','contracts','circles','events']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x29b9a7fbb8995b2423a71cc17cf9810798f6c543',
        output_json_type  = true,
        incremental_column= 'block_timestamp'
    )
}}
