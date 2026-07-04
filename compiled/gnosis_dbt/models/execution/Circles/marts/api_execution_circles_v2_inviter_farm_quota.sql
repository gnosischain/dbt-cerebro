

-- Per-inviter InvitationFarm activity leaderboard: invites claimed + current quota.
-- Passthrough over fct_execution_circles_v2_inviter_farm_quota, ordered by invites_claimed.
SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`contracts_circles_v2_InvitationFarm_events`) AS as_of_date
FROM (
SELECT
    inviter,
    invites_claimed,
    n_claim_events,
    current_quota,
    first_claim_at,
    last_claim_at,
    quota_updated_at
FROM `dbt`.`fct_execution_circles_v2_inviter_farm_quota`
ORDER BY invites_claimed DESC
) AS sub