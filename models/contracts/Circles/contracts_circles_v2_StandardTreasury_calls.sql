{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'contracts', 'circles_v2', 'calls', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ decode_calls(
    tx_table=source('execution', 'transactions'),
    contract_address='0x08f90ab73a515308f03a718257ff9887ed330c6e',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2024-10-01'
) }}
