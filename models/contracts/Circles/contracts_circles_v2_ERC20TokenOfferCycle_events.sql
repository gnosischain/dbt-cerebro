{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(contract_address, block_timestamp, transaction_hash, log_index)',
        unique_key='(contract_address, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'contracts', 'circles_v2', 'events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ decode_logs(
    source_table=source('execution', 'logs'),
    contract_address=[
        '0x76a42aebb2c54d7e259b1c7e4eb0cadf5897a7de',
        '0xb3129372e52b910b6994eaef77bbc1892ea48779',
        '0x68e2c29feed2a4d0f22cc6d271e2b25124d99892'
    ],
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-09-01'
) }}
