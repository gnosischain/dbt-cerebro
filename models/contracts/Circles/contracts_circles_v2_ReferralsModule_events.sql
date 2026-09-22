-- DEPRECATED: no downstream consumers; frozen at cutover, not built by cron (no production tag).
-- To re-enable: restore the production tag and seed with dbt run --full-refresh -s <model>.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['deprecated', 'contracts', 'circles_v2', 'events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ decode_logs(
    source_table=source('execution', 'logs'),
    contract_address='0x12105a9B291aF2ABb0591001155A75949b062CE5',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-11-01'
) }}
