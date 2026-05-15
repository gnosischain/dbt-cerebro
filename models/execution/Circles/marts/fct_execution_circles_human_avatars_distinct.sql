{{
  config(
    materialized='view',
    tags=['production','execution','circles','mixpanel']
  )
}}

-- Per-pseudonym projection of Circles v2 HUMAN avatars (filtered out
-- Groups / Orgs — only direct human registrations participate in the
-- cross-sector user_pseudonym graph). One row per user_pseudonym,
-- enriched with display-name + registration metadata + invite-source
-- flags for grouping.
--
-- ## Cross-sector role
-- `user_pseudonym` is the canonical cross-domain join key produced by
-- `pseudonymize_address(avatar)` — SAME hash space as:
--   * revenue per-user marts (fct_revenue_per_user_*)
--   * gpay user identities (fct_execution_gpay_users_distinct)
--   * gnosis app on-chain users (fct_execution_gnosis_app_users_distinct)
--   * Mixpanel `user_id_hash`
--
-- Closes the loop on the user-pseudonym graph for the Circles flagship
-- product: lets analysts ask "Circles humans who also..." against any
-- other user-keyed sector via the semantic layer.
--
-- ## What's NOT here
-- - Group / Org avatars (these aren't real "users" — they're
--   collective entities, semantically different from human EOAs).
-- - Lending / LP holders (waiting for the UBO models in
--   models/execution/ubo/ to stabilise — protocol-address counts
--   would be misleading).

SELECT
    {{ pseudonymize_address('avatar') }}  AS user_pseudonym,
    registered_at                         AS first_seen_at,
    invited_by IS NOT NULL                AS was_invited,
    -- True iff the avatar published a profile to IPFS — a strong
    -- engagement signal beyond just registration.
    current_metadata_digest IS NOT NULL   AS has_ipfs_profile,
    -- Has the avatar set a display name (either on-chain via NameRegistry
    -- or via the IPFS metadata)? Names are optional for Humans.
    (length(metadata_name) > 0)           AS has_display_name
FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
WHERE avatar_type = 'Human'
  AND avatar IS NOT NULL
