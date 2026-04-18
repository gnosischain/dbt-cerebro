{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(contract_address, block_timestamp, transaction_hash)',
        unique_key='(contract_address, block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'contracts', 'circles_v2', 'calls'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{{ decode_calls(
    tx_table=source('execution', 'transactions'),
    contract_address_ref=ref('contracts_circles_registry'),
    contract_type_filter='ERC20TokenOfferRuntime',
    output_json_type=true,
    incremental_column='block_timestamp',
    start_blocktime='2025-10-01'
) }}
