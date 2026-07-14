{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(contract_address, block_timestamp, transaction_hash, log_index)',
        unique_key='(contract_address, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'contracts', 'circles_v2', 'events', 'scores', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}

-- Decoded events from the OffchainScoreBasedMintPolicy singleton
-- (0x450d68272e43c4cab7cbc7faa37893a50fae9569): PersonalMinted, GroupInitialized,
-- RouterMinted, HistoricalSupply. Whitelisted as CirclesV2ScorePolicy; signatures
-- registered in seeds/event_signatures.csv. Score-based groups began ~2026-05.
{{ decode_logs(
    source_table=source('execution', 'logs'),
    contract_address_ref=ref('contracts_whitelist'),
    contract_type_filter='CirclesV2ScorePolicy',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2026-05-01'
) }}
