-- DEPRECATED: no downstream consumers; frozen at cutover, not built by cron (no production tag).
-- To re-enable: restore the production tag and seed with dbt run --full-refresh -s <model>.
{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy='append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
        tags                    = ['deprecated','contracts','fpmm','events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x9083a2b699c0a4ad06f63580bde2635d26a3eef0',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2020-09-04'
    )
}}
