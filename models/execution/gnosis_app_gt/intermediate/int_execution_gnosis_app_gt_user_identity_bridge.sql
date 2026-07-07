{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'registered_identity'
    }
) }}

-- Internal bridge: registered-identity address -> pseudonym. Uses the shared
-- CEREBRO_PII_SALT (pseudonymize_address), so user_pseudonym is joinable with
-- mixpanel user_id_hash / gpay / circles pseudonyms. Raw address present ->
-- internal-only. A pseudonym-only public view is exposed downstream.
SELECT
    address,
    {{ pseudonymize_address('address') }}   AS user_pseudonym
FROM {{ ref('int_execution_gnosis_app_gt_user_dim') }}
