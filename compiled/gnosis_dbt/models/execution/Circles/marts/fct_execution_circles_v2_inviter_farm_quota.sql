

-- Per-inviter Circles-v2 InvitationFarm ("invitation-at-scale") operational state: how many invites
-- each inviter has CLAIMED from the farm and their latest granted quota. Complements the attribution
-- model int_execution_circles_v2_inviter_canonical (WHO invited whom, via InvitationModule.originInviter)
-- with the farm's inviter activity/allowance. Source events (contracts_circles_v2_InvitationFarm_events):
--   InvitesClaimed(inviter, count)      -> invites_claimed (sum), n_claim_events, first/last claim
--   InviterQuotaUpdated(inviter, quota) -> current_quota (latest by block_timestamp)
-- The farm's other events (BotCreated / FarmGrown) are proxy-bot plumbing, not inviter-level.
-- join_use_nulls: an inviter with quota but no claims (or vice versa) keeps NULL on the missing side
-- rather than a 0/epoch default (see [[feedback_clickhouse_left_join_nulls]]).

WITH claims AS (
    SELECT
        lower(decoded_params['inviter'])                    AS inviter,
        sum(toInt64OrZero(decoded_params['count']))         AS invites_claimed,
        count()                                             AS n_claim_events,
        min(block_timestamp)                                AS first_claim_at,
        max(block_timestamp)                                AS last_claim_at
    FROM `dbt`.`contracts_circles_v2_InvitationFarm_events`
    WHERE event_name = 'InvitesClaimed' AND decoded_params['inviter'] != ''
    GROUP BY inviter
),

quota AS (
    SELECT
        lower(decoded_params['inviter'])                                AS inviter,
        argMax(toInt64OrZero(decoded_params['quota']), block_timestamp) AS current_quota,
        max(block_timestamp)                                            AS quota_updated_at
    FROM `dbt`.`contracts_circles_v2_InvitationFarm_events`
    WHERE event_name = 'InviterQuotaUpdated' AND decoded_params['inviter'] != ''
    GROUP BY inviter
),

inviters AS (
    SELECT DISTINCT lower(decoded_params['inviter']) AS inviter
    FROM `dbt`.`contracts_circles_v2_InvitationFarm_events`
    WHERE event_name IN ('InvitesClaimed', 'InviterQuotaUpdated')
      AND decoded_params['inviter'] != ''
)

SELECT
    i.inviter                          AS inviter,
    coalesce(c.invites_claimed, 0)     AS invites_claimed,
    coalesce(c.n_claim_events, 0)      AS n_claim_events,
    q.current_quota                    AS current_quota,
    c.first_claim_at                   AS first_claim_at,
    c.last_claim_at                    AS last_claim_at,
    q.quota_updated_at                 AS quota_updated_at
FROM inviters i
LEFT JOIN claims c ON c.inviter = i.inviter
LEFT JOIN quota  q ON q.inviter = i.inviter