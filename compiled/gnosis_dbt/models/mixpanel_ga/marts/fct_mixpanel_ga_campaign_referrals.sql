

-- Campaign REFERRAL composition (state-based snapshot) — of each first-touch
-- campaign's identified users, how many became Circles inviters and how many
-- people they brought in. Two scopes, mirroring
-- fct_execution_gnosis_app_gt_referrals (REF-D01):
--   earned       the GA-native paid-reward ledger (earned_from_invite)
--   full_graph   every accepted invite (avatar.invited_by), a ~2.2x superset
-- invites_* counts DISTINCT invitees (people brought in), not ledger edges.
-- Inviter addresses are pseudonymized with the SAME salted macro that hashes
-- Mixpanel distinct_ids (identified distinct_id == on-chain address), so
-- pseudonymize_address(inviter) joins user_id_hash directly and no raw
-- address ever materializes.
-- K-ANONYMITY: campaigns with < 5 all-time signups are bucketed into
-- '_small_campaigns'. Aggregate-only output (no pseudonyms).

WITH signups AS (
    SELECT
        user_id_hash,
        coalesce(first_touch_campaign, 'unknown') AS utm_campaign_raw,
        coalesce(first_touch_source,   'unknown') AS utm_source,
        coalesce(first_touch_medium,   'unknown') AS utm_medium
    FROM `dbt`.`int_mixpanel_ga_user_acquisition`
),

campaign_sizes AS (
    SELECT utm_campaign_raw, count() AS n
    FROM signups
    GROUP BY utm_campaign_raw
),

earned_inviters AS (
    SELECT
        
    sipHash64(concat(unhex('00'), lower(inviter_address)))
 AS user_id_hash,
        uniqExact(invitee_address)                    AS n_invites
    FROM `dbt`.`stg_envio_ga__earned_from_invite`
    GROUP BY user_id_hash
),

graph_inviters AS (
    SELECT
        
    sipHash64(concat(unhex('00'), lower(invited_by)))
 AS user_id_hash,
        uniqExact(avatar_address)                AS n_invites
    FROM `dbt`.`stg_envio_ga__avatars`
    WHERE invited_by != ''
    GROUP BY user_id_hash
),

joined AS (
    SELECT
        if(cs.n < 5, '_small_campaigns', s.utm_campaign_raw) AS utm_campaign,
        s.utm_source                                         AS utm_source,
        s.utm_medium                                         AS utm_medium,
        if(e.user_id_hash != 0, 1, 0)                        AS is_earned_inviter,
        if(e.user_id_hash != 0, e.n_invites, 0)              AS earned_invites,
        if(g.user_id_hash != 0, 1, 0)                        AS is_graph_inviter,
        if(g.user_id_hash != 0, g.n_invites, 0)              AS graph_invites
    FROM signups s
    LEFT JOIN campaign_sizes  cs ON cs.utm_campaign_raw = s.utm_campaign_raw
    LEFT JOIN earned_inviters e  ON e.user_id_hash = s.user_id_hash
    LEFT JOIN graph_inviters  g  ON g.user_id_hash = s.user_id_hash
)

SELECT
    utm_campaign,
    utm_source,
    utm_medium,
    count()                                                             AS signups,
    sum(is_earned_inviter)                                              AS inviters_earned,
    round(sum(is_earned_inviter) / count() * 100, 1)                    AS inviter_pct_earned,
    sum(earned_invites)                                                 AS invites_earned,
    round(sum(earned_invites) / greatest(sum(is_earned_inviter), 1), 2) AS invites_per_inviter_earned,
    sum(is_graph_inviter)                                               AS inviters_full_graph,
    round(sum(is_graph_inviter) / count() * 100, 1)                     AS inviter_pct_full_graph,
    sum(graph_invites)                                                  AS invites_full_graph,
    round(sum(graph_invites) / greatest(sum(is_graph_inviter), 1), 2)   AS invites_per_inviter_full_graph
FROM joined
GROUP BY utm_campaign, utm_source, utm_medium