

-- Current members of a Circles v2 group = the trustees on the group's
-- outgoing trust list (the Circles v2 group-membership semantic), joined
-- to deduped avatar metadata for name/image, with an is_mutual flag when
-- the member also trusts the group back, and (for score-based groups) the
-- member's latest on-chain mint score.
WITH edges AS (
    SELECT lower(truster) AS truster, lower(trustee) AS trustee, valid_from
    FROM `dbt`.`fct_execution_circles_v2_trust_relations_current`
),
scores AS (
    SELECT group_address, member, score
    FROM `dbt`.`api_execution_circles_v2_group_member_scores`
),
meta AS (
    SELECT
        avatar,
        argMax(display_name, registered_at) AS display_name,
        argMax(preview_image_url, registered_at) AS preview_image_url
    FROM (
        SELECT
            lower(avatar) AS avatar,
            coalesce(nullIf(metadata_name, ''), nullIf(name, ''), avatar) AS display_name,
            metadata_preview_image_url AS preview_image_url,
            registered_at
        FROM `dbt`.`api_execution_circles_v2_avatar_metadata`
    )
    GROUP BY avatar
)
SELECT
    today() AS as_of_date,
    e.truster AS group_address,
    e.trustee AS member,
    coalesce(m.display_name, e.trustee) AS display_name,
    m.preview_image_url AS preview_image_url,
    e.valid_from AS member_since,
    (rev.truster IS NOT NULL) AS is_mutual,
    s.score AS score
FROM edges e
LEFT JOIN meta m ON m.avatar = e.trustee
LEFT JOIN edges rev ON rev.truster = e.trustee AND rev.trustee = e.truster
LEFT JOIN scores s ON s.group_address = e.truster AND s.member = e.trustee