{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash, log_index)',
        unique_key              = '(block_timestamp, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        pre_hook                = [
                                    "SET allow_experimental_json_type = 1"
                                ],
        tags                    = ['production','contracts','cow','events']
    )
}}

{# Proxy contract — implementation ABI (GPv2AllowListAuthentication) was
   fetched and stored under the proxy address by fetch_and_insert_abi. #}

{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x2c4c28DDBdAc9C5E7055b4C863b72eA0149D8aFE',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-04-01'
    )
}}
