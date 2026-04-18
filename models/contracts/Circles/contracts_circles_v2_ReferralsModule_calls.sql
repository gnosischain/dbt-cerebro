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
    contract_address='0x12105a9b291af2abb0591001155a75949b062ce5',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-11-01'
) }}
