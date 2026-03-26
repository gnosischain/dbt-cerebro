{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, log_index)',
        unique_key='(block_timestamp, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        pre_hook=["SET allow_experimental_json_type = 1"],
        tags=['dev', 'contracts', 'circles', 'events']
    )
}}

{{ decode_logs(
    source_table=source('execution', 'logs'),
    contract_address='0xFEca40Eb02FB1f4F5F795fC7a03c1A27819B1Ded',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2024-10-01'
) }}
