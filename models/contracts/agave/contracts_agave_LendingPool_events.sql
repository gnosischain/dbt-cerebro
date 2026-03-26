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
        tags                    = ['production','agave','contracts','events']
    )
}}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2022-04-19'
    )
}}

