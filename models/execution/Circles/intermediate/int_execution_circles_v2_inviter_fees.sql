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
--
-- is_gnosis_app_tx flags fees whose mint tx was routed through an active
-- Gnosis App (Cometh 4337) relayer — the canonical "action taken in-app"
-- predicate (same as int_execution_gnosis_app_user_events). The heuristic
-- itself stays ecosystem-wide: any app implementing the same invitation-fee
-- pattern still produces rows, with the flag distinguishing origin so the
-- circles-first economically-active layer can expose both scopes.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH
gnosis_app_txs AS (
    SELECT transaction_hash
    FROM {{ source('execution','transactions') }} tx
    WHERE tx.to_address = '0000000071727de22e5e9d8baf0edac6f37da032'
      AND lower(tx.from_address) IN (
          SELECT lower(replaceAll(address, '0x', ''))
          FROM {{ ref('gnosis_app_relayers') }}
          WHERE is_active = 1
      )
      AND tx.block_timestamp >= toDateTime('2025-11-12')
      AND tx.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(tx.block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(tx.block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('tx.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

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
    toFloat64(t.amount_raw) / pow(10, 18)   AS amount,
    -- isNotNull guard: an unmatched LEFT JOIN yields NULL (not ''), so a bare
    -- `!= ''` returns NULL and silently drops the row from downstream
    -- `is_gnosis_app_tx IN (0,1)` filters. Treat unmatched as 0 (not in-app).
    toUInt8(g.transaction_hash IS NOT NULL AND g.transaction_hash != '') AS is_gnosis_app_tx
FROM wrapper_transfers t
INNER JOIN mint_txs m
    -- mint_events.transaction_hash is UNPREFIXED while wrapper_transfers is
    -- 0x-prefixed; normalize (same pattern as the gnosis_app_txs join below).
    -- A recent upstream format drift silently broke this equality join and
    -- froze the model — keep them format-agnostic.
    ON concat('0x', m.transaction_hash) = t.transaction_hash
INNER JOIN invitee_inviter ii
    ON ii.invitee = t.from_address
   AND ii.inviter = t.to_address
LEFT JOIN gnosis_app_txs g
    ON concat('0x', g.transaction_hash) = t.transaction_hash
