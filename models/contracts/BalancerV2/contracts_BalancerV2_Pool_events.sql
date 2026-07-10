{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(contract_address, block_timestamp, transaction_hash, log_index)',
        unique_key              = '(contract_address, transaction_hash, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        tags                    = ['production', 'contracts', 'balancerv2', 'events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1", "SET max_block_size = 5000"],
        post_hook=["SET allow_experimental_json_type = 0", "SET max_block_size = 65505"]
    )
}}

-- Pool-level Balancer V2 events. The Vault (contracts_BalancerV2_Vault_events)
-- does NOT carry the swap-fee rate; it lives on each pool contract as
-- SwapFeePercentageChanged(uint256) (and ProtocolFeePercentageCacheUpdated for
-- the protocol cut), both inherited from BasePool. event_name_filter pushes a
-- topic0 pre-filter into the raw-log scan so we only read these (rare) config
-- events across all pools, not every pool log.
{{
    decode_logs(
        source_table         = source('execution', 'logs'),
        contract_address_ref = ref('contracts_BalancerV2_pool_registry'),
        contract_type_filter = 'BalancerV2Pool',
        event_name_filter    = ['SwapFeePercentageChanged', 'ProtocolFeePercentageCacheUpdated'],
        output_json_type     = true,
        incremental_column   = 'block_timestamp',
        start_blocktime      = '2021-01-01'
    )
}}
