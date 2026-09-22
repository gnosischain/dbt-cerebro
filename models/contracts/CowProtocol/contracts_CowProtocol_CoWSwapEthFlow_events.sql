-- DEPRECATED: no downstream consumers; frozen at cutover, not built by cron (no production tag).
-- To re-enable: restore the production tag and seed with dbt run --full-refresh -s <model>.
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
        tags                    = ['deprecated','contracts','cow','events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ 
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0xbA3cB449bD2B4ADddBc894D8697F5170800EAdeC',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2023-01-01'
    )
}}
