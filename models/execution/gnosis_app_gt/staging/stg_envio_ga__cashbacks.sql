{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Gnosis Pay cashback NFT-mint program (grain = mint id). A SEPARATE family
-- from the gCRC-transfer gpay_cashback_* models (CASH-D01) — never a drop-in.
-- status is an integer enum (1 = minted, 0 = reverted). ~47% of rows have an
-- empty gnosis_pay_address, so downstream GP-Safe joins drop ~half.
SELECT
    id,
    lower(owner)                                                 AS owner,
    if(gnosis_pay_address = '', NULL, lower(gnosis_pay_address)) AS gnosis_pay_address,
    status,
    toDateTime(minted_at)                                        AS minted_at,
    minted_block
FROM (
    {{ envio_latest('envio_ga', 'cashback', ['owner', 'gnosis_pay_address', 'status', 'minted_at', 'minted_block']) }}
)
