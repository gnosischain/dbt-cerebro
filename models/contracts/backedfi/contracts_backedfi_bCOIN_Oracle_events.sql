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
        tags                    = ['production','contracts','backedfi','events']
    )
}}


{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x69B2D456e8830847f488afDde2a636BFB26218c6',
        output_json_type  = true,
        incremental_column= 'block_timestamp'
    )
}}
