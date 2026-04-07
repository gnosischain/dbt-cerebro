{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET allow_experimental_json_type = 1"],
        tags=['dev', 'contracts', 'circles_v2', 'events']
    )
}}

{{ decode_logs(
    source_table=source('execution', 'logs'),
    contract_address='0x186725d8fe10a573dc73144f7a317fcae5314f19',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-12-01'
) }}
