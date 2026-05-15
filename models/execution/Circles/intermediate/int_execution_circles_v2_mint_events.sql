{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index, batch_index)',
    unique_key='(transaction_hash, log_index, batch_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','mint_events'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- Per-mint event view over int_execution_circles_v2_hub_transfers, tagged
-- with a `mint_kind` so downstream models can distinguish personal mints
-- (humans claiming their hourly issuance) from group mints (group CRC
-- minted to a depositor) and from V1→V2 migrations.
--
-- All three flavours look identical at the ERC-1155 layer — Hub
-- TransferSingle with from = 0x00…00. The classifier combines the
-- operator (msg.sender of the Hub mint call) with the avatar registry:
--
--   * migration - operator is the Circles V2 Migration contract
--                 (contracts_circles_registry.contract_type = 'Migration').
--                 V1 balances are burned on V1 and minted as V2 CRC via
--                 this contract. Two shapes appear in practice:
--                   - self-token (token = recipient): direct V1→V2 mint
--                     to the migrating avatar.
--                   - cross-token (token ≠ recipient): the migration call
--                     bundles routed transfers, so a user's V2 mint can
--                     land on a counterparty within the same call.
--                 The V2 Hub's own `migrate()` ABI entry is essentially
--                 unused (4 historical calls, 0 emitted mints) — the
--                 on-chain path goes through the dedicated Migration
--                 contract instead.
--   * group     - token_avatar_type = 'Group' (token_address registered
--                 as a Group avatar). Covers both self- and cross-token
--                 shapes — group CRC minted to a depositor or routed
--                 via operateFlowMatrix.
--   * personal  - token_avatar_type = 'Human'. Covers BOTH self-token
--                 (direct personalMint claim) AND cross-token (the Hub's
--                 unscheduled mint of pending hourly issuance routed
--                 directly to a counterparty during a transfer). The
--                 token owner is the conceptual minter in both shapes.
--   * other     - residual; should be ~0. Presence indicates a missing
--                 avatar registration row.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH
mints AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        batch_index,
        operator,
        to_address,
        token_id,
        token_address,
        amount_raw,
        transfer_type
    FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
    WHERE from_address = '0x0000000000000000000000000000000000000000'
      AND to_address  != '0x0000000000000000000000000000000000000000'
      {% if start_month and end_month %}
        AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
),

avatar_types AS (
    SELECT
        lower(avatar)    AS avatar,
        any(avatar_type) AS avatar_type
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    GROUP BY avatar
),

migration_operators AS (
    SELECT DISTINCT lower(address) AS address
    FROM {{ ref('contracts_circles_registry') }}
    WHERE contract_type = 'Migration'
)

SELECT
    m.block_number,
    m.block_timestamp,
    m.transaction_hash,
    m.transaction_index,
    m.log_index,
    m.batch_index,
    m.operator,
    m.to_address,
    m.token_id,
    m.token_address,
    m.amount_raw,
    m.transfer_type,
    multiIf(
        mo.address IS NOT NULL,        'migration',
        at.avatar_type = 'Group',      'group',
        at.avatar_type = 'Human',      'personal',
                                       'other'
    ) AS mint_kind
FROM mints m
LEFT JOIN avatar_types        at ON at.avatar = lower(m.token_address)
LEFT JOIN migration_operators mo ON mo.address = lower(m.operator)
