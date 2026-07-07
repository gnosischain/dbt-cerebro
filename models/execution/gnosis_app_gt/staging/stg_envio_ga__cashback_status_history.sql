{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Status transition events for the cashback NFT program (grain = event id).
-- id is an opaque composite key, not an address.
SELECT
    id,
    cashback_id,
    status,
    toDateTime(timestamp)   AS status_at
FROM (
    {{ envio_latest('envio_ga', 'cashback_status_history', ['cashback_id', 'status', 'timestamp']) }}
)
