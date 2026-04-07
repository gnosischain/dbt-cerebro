{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_search','granularity:snapshot']
    )
}}

-- Lightweight (avatar, display_name) lookup used by the dashboard
-- global filter to support searching avatars by display name OR
-- address. Two columns, one row per registered Circles v2 avatar.
-- display_name prefers the IPFS profile name and falls back to the
-- on-chain name (Group/Org only) so every row has something usable
-- to search against; the avatar address is always present.

SELECT
    avatar                                              AS avatar,
    coalesce(
        nullIf(metadata_name, ''),
        nullIf(name,          ''),
        ''
    )                                                   AS display_name
FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
WHERE avatar IS NOT NULL
