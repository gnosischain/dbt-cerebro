{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Registered Circles identity universe (~301k). One row per address.
-- lifetime_cashback unit is UNVERIFIED (CASH-D02) — carried as raw atoms only,
-- no /1e18 KPI until decimals are confirmed by the build owner.
SELECT
    lower(id)                     AS address,
    created_at_block,
    toFloat64(lifetime_cashback)  AS lifetime_cashback_atoms
FROM (
    {{ envio_latest('envio_ga', 'gnosis_app_user', ['created_at_block', 'lifetime_cashback']) }}
)
