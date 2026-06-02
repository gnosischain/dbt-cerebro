{{
    config(
        materialized            = 'incremental',
        incremental_strategy='append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash, log_index)',
        unique_key              = '(block_timestamp, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                  },
        tags                    = ['production','contracts','curve','events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x1337BedC9D22ecbe766dF105c9623922A27963EC',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2021-01-01'
    )
}}
