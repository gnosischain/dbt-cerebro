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
        contract_address  = '0x1332D2b4dc83238B621b7F7D0F72fEc58Bb81A9b',
        output_json_type  = true,
        incremental_column= 'block_timestamp'
    )
}}
