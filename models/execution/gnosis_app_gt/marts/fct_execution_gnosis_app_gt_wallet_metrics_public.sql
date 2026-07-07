{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'mart'],
    meta={'grain': 'canonical_identity'}
) }}

-- PUBLIC per-wallet metric rollup: pseudonym-only boundary over
-- int_execution_gnosis_app_gt_wallet_metrics (same CEREBRO_PII_SALT, joinable to
-- mixpanel/gpay/circles pseudonyms). Drops the raw address; carries the full
-- lifecycle / engagement / trust / segment metric surface.
SELECT
    {{ pseudonymize_address('address') }} AS user_pseudonym,
    * EXCEPT (address)
FROM {{ ref('int_execution_gnosis_app_gt_wallet_metrics') }}
