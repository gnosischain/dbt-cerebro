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
        tags                    = ['production','contracts','swapr','events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'SwaprPool',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2022-03-01'  
    )
}}
