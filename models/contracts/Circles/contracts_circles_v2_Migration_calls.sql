{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, trace_address)',
        unique_key='(block_timestamp, transaction_hash, trace_address)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'contracts', 'circles_v2', 'calls', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
-- Trace-based decoder (source = execution.traces, not execution.transactions).
-- Almost every migrate() call comes through a Safe / AA bundler, so the
-- top-level tx.to_address points at the relayer/Safe, NOT the Migration
-- contract. Reading transactions would catch only ~38 EOA-direct calls;
-- reading traces matched on action_to = Migration contract picks up every
-- internal call (Dune-equivalent counts).
--
-- The decode_calls macro auto-detects traces-mode via the source name and
-- emits an extra trace_address column — required for dedup since a single
-- tx can contain multiple migrate() sub-calls.
{{ decode_calls(
    tx_table=source('execution','traces'),
    contract_address='0xd44b8dcfbadfc78ea64c55b705bfc68199b56376',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2024-10-01'
) }}
