{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Circles auto-topup CONFIG event ledger (grain = opaque event id) — append-only
-- labels {AutoTopupActivated, AutoTopupDeactivated, NewRecipient, ThresholdUpdated}.
-- NOT a Gnosis Pay top-up source. threshold is native BE Int256 -> atoms.
SELECT
    id,
    label,
    lower(owner)            AS owner,
    lower(recipient)        AS recipient,
    lower(recipient_token)  AS recipient_token,
    lower(coordinator)      AS coordinator,
    toFloat64(threshold)    AS threshold_atoms,
    transaction_hash
FROM (
    {{ envio_latest('envio_ga', 'auto_topup', ['label', 'owner', 'recipient', 'recipient_token', 'coordinator', 'threshold', 'transaction_hash']) }}
)
