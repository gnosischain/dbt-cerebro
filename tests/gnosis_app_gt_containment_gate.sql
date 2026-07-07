-- Cutover gate: the heuristic active-user set must be a >= 90% subset of the GT
-- registry (registered = present in the GT registry, the denominator). Returns
-- offending rows only, so the test passes iff containment >= 0.90. avatar
-- containment (the active-user spine) must be near-total (>= 0.99).
SELECT
    registry_containment,
    avatar_containment
FROM {{ ref('int_execution_gnosis_app_gt_user_reconciliation') }}
WHERE registry_containment < 0.90
   OR avatar_containment  < 0.99
