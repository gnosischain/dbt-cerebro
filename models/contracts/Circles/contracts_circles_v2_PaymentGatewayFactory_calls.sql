{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = ('append' if var('start_month', none) else 'delete+insert'),
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, transaction_hash, trace_address)',
        unique_key              = '(block_timestamp, transaction_hash, trace_address)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        pre_hook                = ["SET allow_experimental_json_type = 1"],
        tags                    = ['production', 'contracts', 'circles_v2', 'calls']
    )
}}

{#
  PaymentGatewayFactory calls — decoded via execution.traces because
  createGateway is invoked almost exclusively through a Safe (Gnosis App
  ERC-4337 bundler → Safe.execTransaction → Factory.createGateway).
  `execution.transactions` only records the top-level call (to the Safe),
  so the Factory selector is visible only one hop down in traces.

  Switching to traces gives us all 63 createGateway invocations instead
  of the ~1 top-level call the old source saw. The macro auto-detects
  traces mode and emits an extra `trace_address` column (part of the
  unique_key because one tx can have multiple internal calls to the
  same target).
#}

{{
    decode_calls(
        tx_table          = source('execution','traces'),
        contract_address  = '0x186725d8fe10a573dc73144f7a317fcae5314f19',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2025-12-01'
    )
}}
