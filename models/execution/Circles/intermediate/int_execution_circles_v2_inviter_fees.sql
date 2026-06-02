{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
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
-- Heuristic (matches Dune circles-v2-kpis `weekly_earning_inviter_fee_avatars`):
--   * Same tx_hash as a personal mint (Hub TransferSingle from 0x00…00
--     where the recipient is a Human avatar minting its own Circles — see
--     int_execution_circles_v2_mint_events, mint_kind = 'personal'). The
--     personal-only filter avoids attributing inviter fees to group mints
--     or V1→V2 migrations.
--   * Wrapped-CRC ERC-20 Transfer where `from = invitee` and `to = inviter`,
--     per the avatar→inviter mapping in int_execution_circles_v2_avatars.
--
-- Source: int_execution_circles_v2_wrapper_transfers — the canonical Circles
-- v2 ERC-20 wrapper transfer table. Reading this instead of raw execution.logs
-- avoids scanning every Gnosis-chain Transfer event on full-refresh.
--
-- Amount threshold (>= 1 CRC) is applied downstream by the weekly-earners
-- aggregation, keeping this event-grain model incremental-safe.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH
invitee_inviter AS (
    SELECT
        avatar      AS invitee,
        invited_by  AS inviter
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Human'
      AND invited_by IS NOT NULL
      AND invited_by != '0x0000000000000000000000000000000000000000'
),

mint_txs AS (
    SELECT DISTINCT
        transaction_hash,
        block_timestamp
    FROM {{ ref('int_execution_circles_v2_mint_events') }}
    WHERE mint_kind = 'personal'
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

wrapper_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        token_address,
        from_address,
        to_address,
        amount_raw
    FROM {{ ref('int_execution_circles_v2_wrapper_transfers') }}
    WHERE 1=1
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
)

SELECT
    t.block_number                          AS block_number,
    t.block_timestamp                       AS block_timestamp,
    t.transaction_hash                      AS transaction_hash,
    t.log_index                             AS log_index,
    t.token_address                         AS token_address,
    t.from_address                          AS invitee,
    t.to_address                            AS inviter,
    toFloat64(t.amount_raw) / pow(10, 18)   AS amount
FROM wrapper_transfers t
INNER JOIN mint_txs m
    ON m.transaction_hash = t.transaction_hash
INNER JOIN invitee_inviter ii
    ON ii.invitee = t.from_address
   AND ii.inviter = t.to_address
