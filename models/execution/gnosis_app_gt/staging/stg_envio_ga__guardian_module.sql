{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Append-only module events (grain = opaque event id). Current membership is
-- derived downstream by NETTING Enabled/Disabled per (safe_address,
-- module_address) — NEVER a per-id argMax latest-state mirror (DBT-D05).
-- id is an opaque base-36 event key, NOT an address (do not lower()).
SELECT
    id,
    label,
    lower(safe_address)     AS safe_address,
    lower(module_address)   AS module_address,
    block_number,
    toDateTime(timestamp)   AS event_at,
    transaction_hash
FROM (
    {{ envio_latest(
        'envio_ga', 'guardian_module',
        ['label', 'safe_address', 'module_address', 'block_number', 'timestamp', 'transaction_hash']
    ) }}
)
