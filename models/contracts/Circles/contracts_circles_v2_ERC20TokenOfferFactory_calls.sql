{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET allow_experimental_json_type = 1"],
        tags=['dev', 'contracts', 'circles_v2', 'calls']
    )
}}

{{ decode_calls(
    tx_table=source('execution', 'transactions'),
    contract_address='0x43c8e7cb2fea3a55b52867bb521ebf8cb072feca',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-08-01'
) }}
