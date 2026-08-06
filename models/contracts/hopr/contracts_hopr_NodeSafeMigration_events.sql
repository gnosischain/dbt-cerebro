{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash, log_index)',
        unique_key              = '(block_timestamp, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        tags                    = ['dev','contracts','hopr','events','microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}

/*
  HoprNodeSafeMigration events across both live HOPR networks (dufour + jura).
  ABI resolved per address via contracts_hopr_registry -- dufour -> jura v4 migration tracking. SafeAndModuleMigrationCompleted / DeployedNewV4Module. Expected empty until migration starts.
*/

{{
    decode_logs(
        source_table         = source('execution','logs'),
        contract_address_ref = ref('contracts_hopr_registry'),
        contract_type_filter = 'NodeSafeMigration',
        output_json_type     = true,
        incremental_column   = 'block_timestamp',
        start_blocktime      = '2026-07-27'
    )
}}
