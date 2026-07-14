{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_search','granularity:snapshot']
    )
}}

-- Lightweight (group_address, display_name) lookup used by the Group Explorer
-- global filter to search groups by display name OR paste an address.
-- One row per registered Circles v2 Group avatar (deduped by latest
-- registration, since avatar_metadata can carry >1 registration row).
SELECT
    today() AS as_of_date,
    group_address,
    argMax(display_name, registered_at) AS display_name
FROM (
    SELECT
        lower(avatar) AS group_address,
        coalesce(nullIf(metadata_name, ''), nullIf(name, ''), avatar) AS display_name,
        registered_at
    FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
    WHERE avatar_type = 'Group'
)
GROUP BY group_address
