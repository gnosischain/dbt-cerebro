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
        tags                    = ['production','contracts','ocsdai','events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
-- OpenCover OC-sDAI ("Covered Savings xDAI", CoveredMetavault) decoded event logs.
-- ERC-4626 / async ERC-7540 vault whose asset() is sDAI. Standard ERC-4626
-- Deposit(sender,owner,assets,shares) and Withdraw(...,assets,shares) events are
-- emitted (same signatures as the sDAI vault), which downstream
-- int_yields_ocsdai_share_price_daily uses to reconstruct the share price.
-- ABI registered in seeds/contracts_abi.csv / event_signatures.csv via
-- scripts/signatures/fetch_abi_to_csv.py (see ocsdai_revenue runbook).
{{
    decode_logs(
        source_table      = source('execution','logs'),
        contract_address  = '0x0ac34fe133bde3a2ef589a18a4e10b6a7d253829',
        output_json_type  = true,
        incremental_column= 'block_timestamp',
        start_blocktime   = '2026-03-01'
    )
}}
