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
        tags                    = ['production','spark','contracts','events']
    )
}}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-09-05'
    )
}}

