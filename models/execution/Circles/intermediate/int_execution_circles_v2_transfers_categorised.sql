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
-- For `mint` rows, the `mint_kind` column further distinguishes personal
-- mints, group mints, and V1→V2 migrations — sourced from
-- int_execution_circles_v2_mint_events. NULL for non-mint rows.
--
-- The plan calls for splitting p2p into `p2p_direct` and `p2p_matrix`
-- (matrix-routed via OperatorMatrixFlow → StreamCompleted), but the
-- StreamCompleted event isn't decoded into contracts_circles_v2_Hub_events
-- yet. Once it lands, add a SEMI JOIN against int_execution_circles_v2_stream_completed
-- on (transaction_hash) and split `p2p` into the two subcategories here.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH base AS (
    SELECT *
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
),
mints AS (
    -- Pre-tagged mint flavours; restricted to the same monthly window for
    -- a cheap join.
    SELECT
        transaction_hash,
        log_index,
        batch_index,
        mint_kind
    FROM {{ ref('int_execution_circles_v2_mint_events') }}
    WHERE block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
)
SELECT
    b.block_number,
    b.block_timestamp,
    b.transaction_hash,
    b.transaction_index,
    b.log_index,
    b.batch_index,
    b.transfer_type,
    b.from_address,
    b.to_address,
    b.token_address,
    b.amount_raw,
    b.amount_demurraged_raw,
    multiIf(
        b.transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND b.from_address = '0x0000000000000000000000000000000000000000', 'wrap',
        b.transfer_type = 'CrcV2_ERC20WrapperTransfer'
            AND b.to_address   = '0x0000000000000000000000000000000000000000', 'unwrap',
        b.from_address = '0x0000000000000000000000000000000000000000', 'mint',
        b.to_address   = '0x0000000000000000000000000000000000000000', 'burn',
        'p2p'
    ) AS transfer_category,
    m.mint_kind AS mint_kind
FROM base b
LEFT JOIN mints m
    ON  m.transaction_hash = b.transaction_hash
    AND m.log_index        = b.log_index
    AND m.batch_index      = b.batch_index
