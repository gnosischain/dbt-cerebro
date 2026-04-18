{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'contracts', 'circles_v2', 'calls'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ decode_calls(
    tx_table=source('execution', 'transactions'),
    contract_address='0xd0b5bd9962197beac4cba24244ec3587f19bd06d',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-04-01'
) }}
