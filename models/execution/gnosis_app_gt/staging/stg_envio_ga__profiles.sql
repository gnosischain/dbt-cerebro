{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Circles profile (name / location) per identity. 36 version-superseding
-- duplicate ids exist (CASH round-1), so envio_latest dedup is correctness-required.
SELECT
    lower(id)                   AS address,
    name                        AS profile_name,
    location                    AS profile_location,
    last_updated_block_number
FROM (
    {{ envio_latest('envio_ga', 'profile', ['name', 'location', 'last_updated_block_number']) }}
)
