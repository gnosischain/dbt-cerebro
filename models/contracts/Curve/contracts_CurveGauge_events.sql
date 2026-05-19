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
        tags                    = ['production','contracts','curve','events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0xb721cc32160ab0da2614cc6ab16ed822aeebc101',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-09-01'
    )
}}
