{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        tags                    = ['production','spark','contracts','events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x2Fc8823E1b967D474b47Ae0aD041c2ED562ab588',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-10-06'
    )
}}
