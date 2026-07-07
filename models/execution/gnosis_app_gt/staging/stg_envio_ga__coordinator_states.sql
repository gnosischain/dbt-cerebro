{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Circles auto-topup coordinator current state (grain = coordinator address).
-- One owner per coordinator (1:1). threshold is native BE Int256 -> atoms.
SELECT
    lower(id)               AS coordinator_address,
    lower(owner)            AS owner,
    lower(recipient)        AS recipient,
    lower(recipient_token)  AS recipient_token,
    is_active,
    toFloat64(threshold)    AS threshold_atoms
FROM (
    {{ envio_latest('envio_ga', 'coordinator_state', ['owner', 'recipient', 'recipient_token', 'is_active', 'threshold']) }}
)
