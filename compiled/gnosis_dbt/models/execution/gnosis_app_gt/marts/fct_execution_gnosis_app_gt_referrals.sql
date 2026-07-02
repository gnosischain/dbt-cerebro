

-- Two DISTINCT referral metrics, exposed side by side (REF-D01):
--   earned            = the GA-native paid-reward ledger (earned_from_invite).
--   full_invite_graph = every accepted invite (avatar.invited_by), a ~2.2x
--                       superset that also has a heuristic twin
--                       (int_execution_circles_v2_inviter_canonical).
-- reward_usd is permanently NULL (no token/timestamp; CRC has no price feed).
-- The earned ledger must NEVER be reconciled against circles_v2 inviter_fee.
SELECT
    'earned'                                        AS metric_scope,
    uniqExact(inviter_address)                      AS n_inviters,
    uniqExact(invitee_address)                      AS n_invitees,
    count()                                         AS n_edges,
    round(sum(amount_crc), 2)                       AS total_reward_crc,
    CAST(NULL AS Nullable(Float64))                 AS total_reward_usd
FROM `dbt`.`stg_envio_ga__earned_from_invite`

UNION ALL

SELECT
    'full_invite_graph'                             AS metric_scope,
    uniqExact(invited_by)                           AS n_inviters,
    uniqExact(avatar_address)                       AS n_invitees,
    count()                                         AS n_edges,
    CAST(NULL AS Nullable(Float64))                 AS total_reward_crc,
    CAST(NULL AS Nullable(Float64))                 AS total_reward_usd
FROM `dbt`.`stg_envio_ga__avatars`
WHERE invited_by != ''