

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
    -- Circles protocol version (1 = v1, 2 = v2 — validated 99.9% vs int_execution_circles_v2_avatars)
    version                                         AS circles_version
FROM (
    
SELECT
    id AS id,
    argMax(avatar_type, (_synced_block, ingested_at)) AS avatar_type,
    argMax(invited_by, (_synced_block, ingested_at)) AS invited_by,
    argMax(verification_badge, (_synced_block, ingested_at)) AS verification_badge,
    argMax(is_early_supporter, (_synced_block, ingested_at)) AS is_early_supporter,
    argMax(is_base_group, (_synced_block, ingested_at)) AS is_base_group,
    argMax(profile_id, (_synced_block, ingested_at)) AS profile_id,
    argMax(accepted_invite_timestamp, (_synced_block, ingested_at)) AS accepted_invite_timestamp,
    argMax(timestamp, (_synced_block, ingested_at)) AS timestamp,
    argMax(earned_from_invites, (_synced_block, ingested_at)) AS earned_from_invites,
    argMax(trusts_given_count, (_synced_block, ingested_at)) AS trusts_given_count,
    argMax(trusts_received_count, (_synced_block, ingested_at)) AS trusts_received_count,
    argMax(trusts_mutual_count, (_synced_block, ingested_at)) AS trusts_mutual_count,
    argMax(version, (_synced_block, ingested_at)) AS version
FROM `envio_ga`.`avatar`
GROUP BY id
HAVING max(_deleted) = 0

)