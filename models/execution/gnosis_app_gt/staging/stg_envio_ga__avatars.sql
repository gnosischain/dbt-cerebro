{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Circles avatar state per identity — the active-user containment spine.
-- avatar_type ties on _synced_block for ~35 ids (D08), so dedup with the
-- composite version (_synced_block, ingested_at) for determinism across ALL
-- carried columns.
SELECT
    lower(id)                                       AS avatar_address,
    avatar_type,
    lower(invited_by)                               AS invited_by,
    verification_badge,
    is_early_supporter,
    is_base_group,
    lower(profile_id)                               AS profile_id,
    if(accepted_invite_timestamp = 0, NULL, toDateTime(accepted_invite_timestamp)) AS accepted_invite_at,
    toDateTime(timestamp)                           AS created_at,
    toFloat64(earned_from_invites) / 1e18           AS earned_from_invites_crc,
    -- trust-graph degree counts (already integers on the avatar entity; v2 counts)
    trusts_given_count                              AS trusts_given,
    trusts_received_count                           AS trusts_received,
    trusts_mutual_count                             AS trusts_mutual,
    -- Circles protocol version (1 = v1, 2 = v2), derived from avatar_type. Upstream dropped
    -- the `version` field (envio_ga-indexer PR #1, 2026-09-30) and serves only v2 avatars
    -- since; rows written after that carry version = 0 (column default). On all 175,373 ids
    -- observed before the drop this mapping reproduces `version` exactly. The v1 set is closed.
    toInt64(if(avatar_type IN ('Signup', 'OrganizationSignup', 'Migrating'), 1, 2)) AS circles_version
FROM (
    {{ envio_latest(
        'envio_ga', 'avatar',
        ['avatar_type', 'invited_by', 'verification_badge', 'is_early_supporter',
         'is_base_group', 'profile_id', 'accepted_invite_timestamp', 'timestamp',
         'earned_from_invites', 'trusts_given_count', 'trusts_received_count',
         'trusts_mutual_count'],
        version='(_synced_block, ingested_at)'
    ) }}
)
