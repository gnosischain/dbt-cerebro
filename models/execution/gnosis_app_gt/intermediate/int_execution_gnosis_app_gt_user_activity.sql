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
        'grain': 'canonical_identity',
        'guard': 'One row per GT identity (registry UNION avatar). is_registered_active (registry that did an APP-TAGGED action: swaps/MetriFee/MetriTransfer/pay-topup/cashback/investment/auto-topup) is the CANONICAL GT active-user metric (~26.6k) and the replacement for the old ad-hoc is_engaged. is_registered_active_incl_circles (~40k) additionally counts GENERIC Circles actions (mint/hub/group/wrap/invitation) which carry no app tag — exposed for sizing, NOT the headline. The whole-ecosystem action total (~626k, since 2020) is NEVER a user metric.'
    }
) }}

-- Canonical per-identity ACTIVITY model for the Gnosis App ground-truth suite.
-- Records every action signal per identity so the "Gnosis App user" definition
-- is a CONFIGURABLE combination, not a single guess:
--   * app-feature    : swaps(appCode), cashback, investment, auto-topup config,
--                      pay-topup, referrals
--   * Circles-in-app : MetriFee, MetriTransfer, HubTransfer, PersonalMint,
--                      PrimaryGroupFee, wrap, invitation  (100% identity-mapped)
-- Metri (app.metri.xyz) is the LEGACY Gnosis App, so app_generation rolls
-- gnosis_app(current) + metri(legacy) both up to Gnosis App. Heavy scans live in
-- the two stretch staging tables; everything here reads their compact output.
WITH
identities AS (
    SELECT address FROM {{ ref('stg_envio_ga__users') }}
    UNION DISTINCT
    SELECT avatar_address AS address FROM {{ ref('stg_envio_ga__avatars') }}
),
registry AS (SELECT address FROM {{ ref('stg_envio_ga__users') }}),
heur AS (
    SELECT DISTINCT lower(address) AS address
    FROM {{ ref('int_execution_gnosis_app_users_current') }}
    WHERE address IS NOT NULL AND address != ''
),
sw_gio   AS (SELECT DISTINCT owner AS address FROM {{ ref('stg_envio_ga__swaps') }} WHERE app_scope = 'gnosis_app' AND owner != ''),
sw_metri AS (SELECT DISTINCT owner AS address FROM {{ ref('stg_envio_ga__swaps') }} WHERE app_scope = 'metri' AND owner != ''),
cb       AS (SELECT DISTINCT owner AS address FROM {{ ref('stg_envio_ga__cashbacks') }} WHERE owner != ''),
inv      AS (SELECT DISTINCT owner AS address FROM {{ ref('stg_envio_ga__investment_accounts') }} WHERE owner != ''),
atc      AS (SELECT DISTINCT owner AS address FROM {{ ref('stg_envio_ga__auto_topups') }} WHERE owner != ''),
ref_in   AS (SELECT DISTINCT invitee_address AS address FROM {{ ref('stg_envio_ga__earned_from_invite') }} WHERE invitee_address != ''),
ref_out  AS (SELECT DISTINCT inviter_address AS address FROM {{ ref('stg_envio_ga__earned_from_invite') }} WHERE inviter_address != ''),
-- Circles-in-app + transfer action groups pivoted to per-participant flags
tr AS (
    SELECT
        participant AS address,
        maxIf(1, transfer_type = 'MetriFee')             = 1 AS has_metri_fee,
        maxIf(1, transfer_type = 'MetriTransfer')        = 1 AS has_metri_transfer,
        maxIf(1, transfer_type = 'HubTransfer')          = 1 AS has_hub_transfer,
        maxIf(1, transfer_type = 'PersonalMint')         = 1 AS has_personal_mint,
        maxIf(1, transfer_type = 'PrimaryGroupFee')      = 1 AS has_group_fee,
        maxIf(1, transfer_type = 'Erc20WrapperTransfer') = 1 AS has_wrap,
        maxIf(1, transfer_type = 'InvitationFee')        = 1 AS has_invitation,
        maxIf(1, transfer_type IN ('PayTopUp', 'AutoTopup')) = 1 AS has_pay_topup
    FROM {{ ref('stg_envio_ga__transfer_actions') }}
    GROUP BY participant
),
ts AS (
    SELECT address, first_action_at, last_action_at, n_actions
    FROM {{ ref('stg_envio_ga__transaction_actions') }}
),
flags AS (
    SELECT
        i.address                                                  AS address,
        i.address IN (SELECT address FROM registry)                AS is_registered,
        -- app-feature signals
        i.address IN (SELECT address FROM sw_gio)                  AS has_swap_gnosis_io,
        i.address IN (SELECT address FROM sw_metri)                AS has_swap_metri,
        i.address IN (SELECT address FROM cb)                      AS has_cashback,
        i.address IN (SELECT address FROM inv)                     AS has_investment,
        i.address IN (SELECT address FROM atc)                     AS has_auto_topup,
        i.address IN (SELECT address FROM ref_in)                  AS has_referral_earned,
        i.address IN (SELECT address FROM ref_out)                 AS has_referral_sent,
        -- Circles-in-app action signals (from the stretch transfer staging)
        coalesce(tr.has_metri_fee, false)                          AS has_metri_fee,
        coalesce(tr.has_metri_transfer, false)                     AS has_metri_transfer,
        coalesce(tr.has_hub_transfer, false)                       AS has_hub_transfer,
        coalesce(tr.has_personal_mint, false)                      AS has_personal_mint,
        coalesce(tr.has_group_fee, false)                          AS has_group_fee,
        coalesce(tr.has_wrap, false)                               AS has_wrap,
        coalesce(tr.has_invitation, false)                         AS has_invitation,
        coalesce(tr.has_pay_topup, false)                          AS has_pay_topup,
        -- heuristic cross-reference + recency
        i.address IN (SELECT address FROM heur)                    AS is_heuristic_active,
        ts.first_action_at                                         AS first_action_at,
        ts.last_action_at                                          AS last_action_at,
        coalesce(ts.n_actions, 0)                                  AS n_actions
    FROM identities i
    LEFT JOIN tr ON i.address = tr.address
    LEFT JOIN ts ON i.address = ts.address
)
SELECT
    *,
    -- current app (app.gnosis.io) vs legacy app (Metri) signals
    (has_swap_gnosis_io OR is_heuristic_active OR has_cashback OR has_pay_topup)          AS current_app_signal,
    (has_swap_metri OR has_metri_fee OR has_metri_transfer OR has_investment OR has_auto_topup) AS legacy_app_signal,
    multiIf(
        (has_swap_gnosis_io OR is_heuristic_active OR has_cashback OR has_pay_topup)
          AND (has_swap_metri OR has_metri_fee OR has_metri_transfer OR has_investment OR has_auto_topup), 'both',
        (has_swap_metri OR has_metri_fee OR has_metri_transfer OR has_investment OR has_auto_topup),        'legacy',
        (has_swap_gnosis_io OR is_heuristic_active OR has_cashback OR has_pay_topup),                       'current',
        'none'
    )                                                                                    AS app_generation,
    -- app-tagged activity (unambiguously routed through Gnosis App / Metri)
    (has_swap_gnosis_io OR has_swap_metri OR has_metri_fee OR has_metri_transfer
     OR has_pay_topup OR has_cashback OR has_investment OR has_auto_topup)               AS is_app_active,
    -- Circles / social actions (only meaningful as app usage for a registered identity)
    (has_hub_transfer OR has_personal_mint OR has_group_fee OR has_wrap
     OR has_invitation OR has_referral_earned OR has_referral_sent)                      AS is_circles_active,
    -- CANONICAL GT active-user metric: registered AND did an APP-TAGGED action
    -- (unambiguously routed through Gnosis App / Metri — includes the app-routed
    -- Circles actions MetriFee/MetriTransfer). ~26.6k, the plan anchor.
    (is_registered
     AND (has_swap_gnosis_io OR has_swap_metri OR has_metri_fee OR has_metri_transfer
          OR has_pay_topup OR has_cashback OR has_investment OR has_auto_topup))         AS is_registered_active,
    -- BROAD variant: also counts GENERIC Circles actions (PersonalMint/HubTransfer/
    -- PrimaryGroupFee/wrap/invitation) + referrals by a registered identity. These
    -- carry NO app tag (any Circles client), so this OVER-reaches "used the app"
    -- (~40k) — exposed for sizing, not the headline.
    (is_registered
     AND (has_swap_gnosis_io OR has_swap_metri OR has_metri_fee OR has_metri_transfer
          OR has_pay_topup OR has_cashback OR has_investment OR has_auto_topup
          OR has_hub_transfer OR has_personal_mint OR has_group_fee OR has_wrap
          OR has_invitation OR has_referral_earned OR has_referral_sent))                AS is_registered_active_incl_circles,
    -- linked to a Gnosis Pay card (cashback owner or top-up funder)
    (has_cashback OR has_pay_topup)                                                      AS is_gp_card_user
FROM flags
