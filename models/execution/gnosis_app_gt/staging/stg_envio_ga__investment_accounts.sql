{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'staging', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': false, 'privacy_tier': 'internal', 'api': {'exclude_from_api': true}}
) }}

-- Metri auto-invest accounts (grain = account address). account_address is a
-- different address space from owner (join on owner, never account_address).
-- A synthetic tombstone (id='zz_fake_del_test', _deleted=1 at block 0) is
-- dropped by the envio_latest max(_deleted)=0 guard.
SELECT
    lower(id)                   AS account_address,
    lower(owner)                AS owner,
    lower(coordinator)          AS coordinator,
    lower(investment_token)     AS investment_token,
    is_active
FROM (
    {{ envio_latest('envio_ga', 'investment_account', ['owner', 'coordinator', 'investment_token', 'is_active']) }}
)
