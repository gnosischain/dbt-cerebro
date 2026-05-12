{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index, batch_index)',
    unique_key='(transaction_hash, log_index, batch_index, transfer_category)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','transfers','categorised']
  )
}}

-- Per-transfer categorisation. Wraps int_execution_circles_v2_transfers and
-- tags each row with one of five `transfer_category` values:
--
--   mint        - Hub ERC-1155 TransferSingle, from = 0x00..00
--   burn        - Hub ERC-1155 TransferSingle, to   = 0x00..00
--   wrap        - Wrapper ERC-20 Transfer,     from = 0x00..00
--   unwrap      - Wrapper ERC-20 Transfer,     to   = 0x00..00
--   p2p         - any other transfer (peer-to-peer)
--
-- The plan calls for splitting p2p into `p2p_direct` and `p2p_matrix`
-- (matrix-routed via OperatorMatrixFlow → StreamCompleted), but the
-- StreamCompleted event isn't decoded into contracts_circles_v2_Hub_events
-- yet. Once it lands, add a SEMI JOIN against int_execution_circles_v2_stream_completed
-- on (transaction_hash) and split `p2p` into the two subcategories here.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    transfer_type,
    from_address,
    to_address,
    token_address,
    amount_raw,
    amount_demurraged_raw,
    multiIf(
        transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND from_address = '0x0000000000000000000000000000000000000000', 'wrap',
        transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND to_address   = '0x0000000000000000000000000000000000000000', 'unwrap',
        from_address = '0x0000000000000000000000000000000000000000', 'mint',
        to_address   = '0x0000000000000000000000000000000000000000', 'burn',
        'p2p'
    ) AS transfer_category
FROM {{ ref('int_execution_circles_v2_transfers') }}
WHERE block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='block_timestamp',
          destination_field='block_timestamp',
          add_and=True) }}
  {% endif %}
