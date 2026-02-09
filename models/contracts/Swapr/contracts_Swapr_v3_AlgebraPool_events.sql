{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(contract_address, block_timestamp, transaction_hash, log_index)',
        unique_key              = '(contract_address, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['dev','contracts','swapr','events']
    )
}}

{# 
    Pool addresses are dynamically selected from contracts_whitelist seed
    based on whitelisted tokens (both token0 and token1 must be whitelisted)
    Filtered to SwaprPool contract type only
#}

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
