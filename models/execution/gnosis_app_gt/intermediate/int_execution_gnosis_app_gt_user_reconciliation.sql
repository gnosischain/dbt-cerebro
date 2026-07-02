{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='tuple()',
    tags=['production', 'execution', 'gnosis_app_gt', 'mart'],
    meta={'grain': 'reconciliation_snapshot'}
) }}

-- Aggregate-only reconciliation (no addresses -> public).
--   registry (301k) = the raw Circles-identity registry (ecosystem-wide; NOT users).
--   gt_registered_active (~26.6k) = the CANONICAL GT active-user metric from
--     int_..._user_activity (registered + any APP-TAGGED action incl. MetriFee/
--     MetriTransfer); gt_registered_active_incl_circles (~40k) adds generic
--     Circles actions (no app tag) — sizing only. gt_active_missed_by_heuristic
--     (~17.5k) = legacy/non-current users the current-app heuristic can't see.
--   the ACTIVE-user containment spine is the avatar table (NOT the registry).
-- Gate: registry_containment >= 0.90; avatar_containment ~1.0 (99.98%).
WITH heur AS (
    SELECT DISTINCT lower(address) AS addr
    FROM {{ ref('int_execution_gnosis_app_users_current') }}
    WHERE address IS NOT NULL AND address != ''
),
counted AS (
    SELECT
        (SELECT count() FROM heur)                                          AS heuristic_users,
        (SELECT count() FROM {{ ref('stg_envio_ga__users') }})              AS gt_registry_users,
        (SELECT count() FROM {{ ref('stg_envio_ga__avatars') }})            AS gt_avatar_users,
        -- canonical GT active-user metrics from the activity model (registered +
        -- any app-tagged action = 26.6k; incl-circles broad variant = 40k)
        (SELECT countIf(is_registered_active) FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }})               AS gt_registered_active,
        (SELECT countIf(is_registered_active_incl_circles) FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }})  AS gt_registered_active_incl_circles,
        (SELECT countIf(is_registered_active AND is_heuristic_active) FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }})     AS gt_active_in_heuristic,
        (SELECT countIf(is_registered_active AND NOT is_heuristic_active) FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }}) AS gt_active_missed_by_heuristic,
        (SELECT countIf(is_registered_active AND legacy_app_signal AND NOT current_app_signal) FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }}) AS gt_active_legacy_only,
        countIf(addr IN (SELECT address FROM {{ ref('stg_envio_ga__users') }}))          AS heuristic_in_registry,
        countIf(addr IN (SELECT avatar_address FROM {{ ref('stg_envio_ga__avatars') }})) AS heuristic_in_avatar
    FROM heur
)
SELECT
    heuristic_users,
    gt_registry_users,
    gt_avatar_users,
    gt_registered_active,
    gt_registered_active_incl_circles,
    gt_active_in_heuristic,
    gt_active_missed_by_heuristic,
    gt_active_legacy_only,
    heuristic_in_registry,
    heuristic_in_avatar,
    heuristic_users - heuristic_in_registry                       AS heuristic_only_vs_registry,
    round(heuristic_in_registry / heuristic_users, 4)             AS registry_containment,
    round(heuristic_in_avatar   / heuristic_users, 4)             AS avatar_containment,
    round(gt_registered_active  / heuristic_users, 4)             AS active_vs_heuristic_ratio
FROM counted
