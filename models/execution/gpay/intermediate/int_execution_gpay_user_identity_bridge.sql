{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym, identity_role, address)',
    settings={'allow_nullable_key': 1},
    tags=['internal_only', 'privacy:tier_internal', 'execution', 'gpay'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}
  )
}}

-- INTERNAL ONLY — GP-side analogue of int_execution_gnosis_app_user_identity_bridge.
-- Holds raw GP addresses (Safe contracts, owners, delegates) paired with their
-- user_pseudonym and identity_role. Used by GP-side downstream models to
-- pseudonymize raw addresses at build time without recomputing the hash.
-- Never exposed to MCP or API.
--
-- Mirrors the existing pattern in int_execution_gpay_safe_identities, but
-- keeps the raw `address` column alongside `user_pseudonym` (the existing
-- model exposes pseudonym only and is what marts read). After this bridge
-- lands, int_execution_gpay_safe_identities should be refactored to SELECT
-- FROM this bridge (drop the join + hash recomputation).

WITH gp_safes AS (
    SELECT lower(address) AS gp_safe FROM {{ ref('int_execution_gpay_wallets') }}
),

initial_owners AS (
    SELECT
        lower(oe.owner)                              AS address,
        {{ pseudonymize_address('oe.owner') }}       AS user_pseudonym,
        'initial_owner'                              AS identity_role,
        oe.safe_address                              AS gp_safe,
        toDateTime64(oe.block_timestamp, 0, 'UTC')   AS first_seen_at,
        toDateTime64(oe.block_timestamp, 0, 'UTC')   AS last_seen_at
    FROM {{ ref('int_execution_safes_owner_events') }} oe
    INNER JOIN gp_safes gs ON lower(oe.safe_address) = gs.gp_safe
    WHERE oe.event_kind = 'safe_setup'
      AND oe.owner IS NOT NULL
),

delegates AS (
    SELECT
        lower(d.delegate_address)                            AS address,
        {{ pseudonymize_address('d.delegate_address') }}     AS user_pseudonym,
        'delegate'                                           AS identity_role,
        d.gp_safe                                            AS gp_safe,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC')))         AS first_seen_at,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC')))         AS last_seen_at
    FROM {{ ref('int_execution_gpay_spender_delegates_current') }} d
),

safe_self AS (
    SELECT
        gs.gp_safe                              AS address,
        {{ pseudonymize_address('gs.gp_safe') }} AS user_pseudonym,
        'safe_self'                             AS identity_role,
        gs.gp_safe                              AS gp_safe,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC'))) AS first_seen_at,
        CAST(NULL AS Nullable(DateTime64(0, 'UTC'))) AS last_seen_at
    FROM gp_safes gs
)

SELECT * FROM initial_owners
UNION ALL
SELECT * FROM delegates
UNION ALL
SELECT * FROM safe_self
