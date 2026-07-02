{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(user_pseudonym)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'execution', 'gnosis_app_gt', 'mart']
) }}

-- PUBLIC identity boundary: pseudonym-only (no raw address). The join surface
-- to mixpanel user_id_hash / gpay / circles (same CEREBRO_PII_SALT). Carries
-- only non-identifying registry attributes + engagement flags so consumers can
-- filter to the CANONICAL engaged population (is_engaged ~26.6k, sourced from the
-- activity model) and to Gnosis-App-only (has_ga_signal) vs Metri (has_metri_signal).
SELECT
    b.user_pseudonym,
    d.user_segment,
    d.has_circles_avatar,
    d.has_profile,
    d.has_lifetime_cashback,
    d.is_engaged,
    d.is_heuristic_active,
    (d.is_heuristic_active OR d.has_swapped_gnosis_app OR d.has_cashback OR d.has_lifetime_cashback) AS has_ga_signal,
    (d.has_swapped_metri OR d.has_investment)                                                        AS has_metri_signal
FROM {{ ref('int_execution_gnosis_app_gt_user_identity_bridge') }} b
INNER JOIN {{ ref('int_execution_gnosis_app_gt_user_dim') }} d USING (address)
