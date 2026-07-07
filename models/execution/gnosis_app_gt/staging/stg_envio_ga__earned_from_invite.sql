{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- GA-native invite-reward ledger (grain = opaque event id; ~5 duplicate ids
-- exist so dedup is required). amount is native BE Int256 CRC (toFloat64/1e18;
-- the "little-endian blob" note was refuted). This is a DIFFERENT source from
-- the circles_v2 inviter_fee stream — must never be reconciled against it.
SELECT
    id,
    lower(invitee_id)               AS invitee_address,
    lower(inviter_id)               AS inviter_address,
    toFloat64(amount) / 1e18        AS amount_crc
FROM (
    {{ envio_latest('envio_ga', 'earned_from_invite', ['amount', 'invitee_id', 'inviter_id']) }}
)
