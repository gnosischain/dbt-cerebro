{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(avatar)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','inviter'],
    pre_hook=["SET join_use_nulls = 1", "SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_use_nulls = 0", "SET join_algorithm = 'default'"]
  )
}}

-- Canonical inviter per Circles v2 Human avatar, resolving the invitation-at-scale
-- "farm". Hub.RegisterHuman records inviter = the farm's proxyInviter (infrastructure),
-- not the human referrer. The GA InvitationModule emits RegisterHuman with
-- (human, proxyInviter, originInviter); we remap by the INVITEE (human = avatar),
-- which is exact:
--   * proxy -> origin is many-to-one (880 proxies map to >1 origin, up to 8), so a
--     proxyInviter-dictionary join would mis-assign;
--   * 2,537 avatars are invited DIRECTLY by an address that also proxies elsewhere
--     and must NOT be remapped -- the per-invitee join leaves them alone.
-- Of ~25k Human registrations, 12,808 are farm registrations remapped to origin.
-- One row per avatar (both sides aggregated) so downstream LEFT JOINs never fan out.
--
-- Consumed by int_execution_circles_v2_avatars (invited_by) and
-- int_execution_gnosis_app_user_events (rule_invite_human); everything else inherits.

{% set zero = '0x0000000000000000000000000000000000000000' %}

WITH farm AS (
    SELECT
        lower(decoded_params['human'])               AS human,
        any(lower(decoded_params['originInviter']))  AS origin_inviter
    FROM {{ ref('contracts_circles_v2_InvitationModule_events') }}
    WHERE event_name = 'RegisterHuman'
    GROUP BY human
),

hub AS (
    SELECT
        lower(decoded_params['avatar'])   AS avatar,
        any(lower(decoded_params['inviter'])) AS raw_inviter
    FROM {{ ref('contracts_circles_v2_Hub_events') }}
    WHERE event_name = 'RegisterHuman'
    GROUP BY avatar
)

SELECT
    h.avatar                            AS avatar,
    h.raw_inviter                       AS raw_inviter,
    f.origin_inviter                    AS origin_inviter,
    if(f.origin_inviter IS NOT NULL AND f.origin_inviter NOT IN ('', '{{ zero }}'),
       f.origin_inviter,
       h.raw_inviter)                   AS canonical_inviter,
    f.origin_inviter IS NOT NULL        AS via_farm
FROM hub h
LEFT JOIN farm f
    ON f.human = h.avatar
