{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
    unique_key='(transaction_hash, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','inviter_fees'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- Per-event inviter-fee transfers paid in wrapped CRC during a personal-mint
-- transaction. Used downstream by the Gnosis App Weekly Economically Active
-- Users (WEAU) definition: an inviter "earns" a fee in a given week if at
-- least 1 CRC of inviter fee landed in that week.
--
-- Heuristic (matches the Dune circles-v2-kpis dashboard):
--   * Same tx_hash as a Hub.PersonalMint event (i.e., the mint that triggers
--     the inviter-fee payout — encoded here as Hub TransferSingle with
--     from_address = 0x00…00).
--   * Wrapped-CRC ERC-20 Transfer where `from = invitee` and `to = inviter`,
--     per the avatar→inviter mapping in int_execution_circles_v2_avatars.
--
-- Amount threshold (>= 1 CRC) is applied downstream by the weekly-earners
-- aggregation, keeping this event-grain model incremental-safe.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{% set logs_pre_filter %}
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND block_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
    {% endif %}
{% endset %}

WITH
-- Avatar → inviter mapping (humans only, with a non-null inviter).
invitee_inviter AS (
    SELECT
        avatar      AS invitee,
        invited_by  AS inviter
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
),

-- Transactions where a personal mint happened (one row per tx).
mint_txs AS (
    SELECT DISTINCT
        transaction_hash,
        block_timestamp
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% endif %}
),

-- Raw ERC-20 Transfer logs scoped to the lookback window. Decode topic1/2
-- into from/to and the data slot into the raw amount. We don't filter by
-- token here — any ERC-20 transfer matching the invitee→inviter pair in a
-- mint tx is a candidate; the join below restricts to the right token by
-- requiring the from-address to be the invitee.
erc20_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        concat('0x', transaction_hash)                                AS transaction_hash,
        log_index,
        concat('0x', address)                                         AS token_address,
        concat('0x', substring(topic1, 27, 40))                       AS from_address,
        concat('0x', substring(topic2, 27, 40))                       AS to_address,
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64))))  AS amount_raw
    FROM (
        {{ dedup_source(
              source_ref=source('execution', 'logs'),
              partition_by='block_number, transaction_index, log_index',
              columns='block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp',
              pre_filter=logs_pre_filter
        ) }}
    )
)

SELECT
    t.block_number,
    t.block_timestamp,
    t.transaction_hash,
    t.log_index,
    t.token_address,
    t.from_address  AS invitee,
    t.to_address    AS inviter,
    toFloat64(t.amount_raw) / pow(10, 18) AS amount
FROM erc20_transfers t
INNER JOIN mint_txs m
    ON m.transaction_hash = t.transaction_hash
INNER JOIN invitee_inviter ii
    ON ii.invitee = t.from_address
   AND ii.inviter = t.to_address
