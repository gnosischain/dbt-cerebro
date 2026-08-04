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
  HoprChannels events for BOTH live networks (dufour + jura).

  The ABI is resolved per contract address via contracts_hopr_registry, which is
  what makes the dual-deployment decode correct: dufour and jura emit the same
  logical events under different topic0s, and a single-ABI decode would silently
  drop one network. All 14 topic0s across the two deployments were verified
  against keccak of their canonical signatures before this model was written.

  Downstream note: both deployments emit the NEW channel balance, never a delta.
  Redeemed value therefore requires a per-channel window diff -- see
  int_hopr_channels_events. jura additionally packs the whole Channel struct into
  the `channel` bytes32 (balance uint96 | ticketIndex uint48 | closureTime uint32
  | epoch uint24 | status uint8), which is unpacked there too.
*/

{{
    decode_logs(
        source_table         = source('execution','logs'),
        contract_address_ref = ref('contracts_hopr_registry'),
        contract_type_filter = 'Channels',
        output_json_type     = true,
        incremental_column   = 'block_timestamp',
        start_blocktime      = '2023-08-29'
    )
}}
