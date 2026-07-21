{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    title,
    slug,
    category_id,
    -- GIP number parsed from the topic title (links to snapshot_proposals).
    toUInt32OrNull(extract(title, 'GIP[ -]?0*([0-9]+)')) AS gip_number,
    -- Governance lifecycle phase from the tags string (phase-1 discussion ->
    -- phase-2 temp check -> phase-3 Snapshot vote). 'none' when untagged.
    multiIf(
        position(tags, 'phase-3') > 0, 'phase-3',
        position(tags, 'phase-2') > 0, 'phase-2',
        position(tags, 'phase-1') > 0, 'phase-1',
        'none'
    )                                                    AS phase,
    posts_count,
    reply_count,
    views,
    like_count,
    participant_count,
    tags,
    created_at,
    last_posted_at,
    bumped_at,
    closed,
    archived,
    pinned,
    ingested_at
FROM {{ source('governance', 'forum_topics') }} FINAL
