{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'offers']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    lower(contract_address) AS cycle_address,
    event_name,
    decoded_params
FROM {{ ref('contracts_circles_v2_ERC20TokenOfferCycle_events') }}
WHERE event_name IN ('CycleConfiguration', 'NextOfferCreated', 'NextOfferTokensDeposited', 'OfferClaimed', 'OfferTrustSynced', 'UnclaimedTokensWithdrawn')
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}
