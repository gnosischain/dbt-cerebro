{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_trust_network', 'granularity:snapshot']
    )
}}

-- Trust-network edge list for the Circles v2 Avatar Trust Network
-- panel.
--
-- One row per directed trust edge from the perspective of the focal
-- avatar. For mutual relationships we emit TWO rows (avatar →
-- counterparty AND counterparty → avatar) so the chart can render
-- both arrows. The dashboard's avatar global filter scopes rows to
-- one focal avatar — ClickHouse pushes that filter down into each
-- UNION branch, so per-request cost stays small despite the apparent
-- size of the query. Materialising this globally would produce
-- ~millions of rows × long IPFS strings and blow the memory budget,
-- so this one stays a view on purpose.
--
-- Each row carries display name and IPFS preview image for both
-- endpoints. The dashboard graph chart consumes these via
-- sourceImageField / targetImageField to render avatar thumbnails on
-- the nodes.
--
-- The `target_layer` column is used by the chart's concentric layout:
-- "Focal avatar" lives at the centre, "Mutual" forms the inner ring,
-- "Trust given" the middle ring, "Trust received" the outer ring.

WITH meta AS (
    SELECT
        avatar,
        coalesce(
            nullIf(metadata_name, ''),
            nullIf(name, ''),
            concat(substring(avatar, 1, 6), '…', substring(avatar, -4))
        )                                                           AS display_name,
        coalesce(metadata_preview_image_url, metadata_image_url, '') AS image_url
    FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
),
edges AS (
    -- Outgoing edges (trust given): focal avatar → counterparty
    SELECT
        tr.avatar         AS avatar,
        tr.avatar         AS source_id,
        tr.counterparty   AS target_id,
        'Trust given'     AS direction,
        2                 AS direction_order
    FROM {{ ref('api_execution_circles_v2_avatar_trust_relations') }} tr
    WHERE tr.avatar IS NOT NULL
      AND tr.counterparty IS NOT NULL
      AND tr.direction = 'outgoing'

    UNION ALL

    -- Incoming edges (trust received): counterparty → focal avatar
    SELECT
        tr.avatar         AS avatar,
        tr.counterparty   AS source_id,
        tr.avatar         AS target_id,
        'Trust received'  AS direction,
        3                 AS direction_order
    FROM {{ ref('api_execution_circles_v2_avatar_trust_relations') }} tr
    WHERE tr.avatar IS NOT NULL
      AND tr.counterparty IS NOT NULL
      AND tr.direction = 'incoming'

    UNION ALL

    -- Mutual edges, FIRST direction: focal avatar → counterparty
    SELECT
        tr.avatar         AS avatar,
        tr.avatar         AS source_id,
        tr.counterparty   AS target_id,
        'Mutual'          AS direction,
        1                 AS direction_order
    FROM {{ ref('api_execution_circles_v2_avatar_trust_relations') }} tr
    WHERE tr.avatar IS NOT NULL
      AND tr.counterparty IS NOT NULL
      AND tr.direction = 'mutual'

    UNION ALL

    -- Mutual edges, SECOND direction: counterparty → focal avatar
    SELECT
        tr.avatar         AS avatar,
        tr.counterparty   AS source_id,
        tr.avatar         AS target_id,
        'Mutual'          AS direction,
        1                 AS direction_order
    FROM {{ ref('api_execution_circles_v2_avatar_trust_relations') }} tr
    WHERE tr.avatar IS NOT NULL
      AND tr.counterparty IS NOT NULL
      AND tr.direction = 'mutual'
)
SELECT
    e.avatar                                          AS avatar,
    e.source_id                                       AS source_id,
    e.target_id                                       AS target_id,
    src.display_name                                  AS source_name,
    tgt.display_name                                  AS target_name,
    src.image_url                                     AS source_image,
    tgt.image_url                                     AS target_image,
    e.direction                                       AS direction,
    -- Node category drives the concentric ring assignment. The
    -- counterparty node always carries the "ring" (Mutual / Trust
    -- given / Trust received); the focal avatar always carries
    -- "Focal avatar" (ring 0 = centre).
    if(e.source_id = e.avatar, 'Focal avatar', e.direction) AS source_layer,
    if(e.target_id = e.avatar, 'Focal avatar', e.direction) AS target_layer,
    1                                                 AS value
FROM edges e
LEFT JOIN meta src ON src.avatar = e.source_id
LEFT JOIN meta tgt ON tgt.avatar = e.target_id
ORDER BY e.direction_order, e.source_id, e.target_id
