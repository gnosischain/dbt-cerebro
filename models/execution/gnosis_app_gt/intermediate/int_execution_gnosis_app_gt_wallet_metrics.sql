{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={'allow_nullable_key': 1},
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"],
    tags=['production', 'execution', 'gnosis_app_gt', 'internal_only', 'privacy:tier_internal'],
    meta={
        'expose_to_mcp': false,
        'privacy_tier': 'internal',
        'api': {'exclude_from_api': true},
        'grain': 'canonical_identity',
        'guard': 'Per-wallet metric rollup (one row per registry-or-avatar identity). COUNT/LIFECYCLE only — no money except the validated earned_from_invites_crc / referral_crc_earned. Lifecycle (tenure/recency/active) derives from transaction_action.timestamp; the raw registry is NEVER an active denominator. join_use_nulls=1 so unmatched enrichments are NULL/0, not the DateTime epoch default.'
    }
) }}

-- WALLET METRICS: the per-identity analytical rollup the _gt sector was missing.
-- Spine = the canonical activity model (flags + first/last action ts + generation);
-- enriched with per-action-type COUNTS from the compact staging (no new heavy
-- scan), trust-graph degree + referral rewards from the avatar/invite ledgers, and
-- derived lifecycle + engagement segments. Money is limited to the two decode-
-- validated CRC fields; balances/revenue are the P0-gated follow-on.
WITH
tr AS (
    SELECT participant AS address,
        sumIf(n_events, transfer_type = 'MetriTransfer')              AS n_metri_transfer,
        sumIf(n_events, transfer_type = 'MetriFee')                   AS n_metri_fee,
        sumIf(n_events, transfer_type = 'HubTransfer')                AS n_hub_transfer,
        sumIf(n_events, transfer_type = 'PersonalMint')               AS n_personal_mint,
        sumIf(n_events, transfer_type = 'PrimaryGroupFee')            AS n_group_fee,
        sumIf(n_events, transfer_type IN ('PayTopUp', 'AutoTopup'))   AS n_pay_topup,
        sum(n_events)                                                 AS n_transfer_events
    FROM {{ ref('stg_envio_ga__transfer_actions') }}
    GROUP BY participant
),
sw AS (
    SELECT owner AS address,
        countIf(app_scope = 'gnosis_app') AS n_swaps_gnosis_app,
        countIf(app_scope = 'metri')      AS n_swaps_metri
    FROM {{ ref('stg_envio_ga__swaps') }}
    WHERE owner != '' AND app_scope IN ('gnosis_app', 'metri')
    GROUP BY owner
),
cb  AS (SELECT owner AS address, count() AS n_cashback   FROM {{ ref('stg_envio_ga__cashbacks') }}           WHERE owner != '' GROUP BY owner),
inv AS (SELECT owner AS address, count() AS n_investment FROM {{ ref('stg_envio_ga__investment_accounts') }} WHERE owner != '' GROUP BY owner),
ref AS (
    SELECT inviter_address AS address,
        uniqExact(invitee_address) AS n_invitees,
        sum(amount_crc)            AS referral_crc_earned
    FROM {{ ref('stg_envio_ga__earned_from_invite') }}
    WHERE inviter_address != ''
    GROUP BY inviter_address
),
av AS (
    SELECT avatar_address AS address, trusts_given, trusts_received, trusts_mutual,
           earned_from_invites_crc, circles_version
    FROM {{ ref('stg_envio_ga__avatars') }}
),
base AS (
    SELECT
        a.address                AS address,
        a.app_generation         AS app_generation,
        a.is_registered          AS is_registered,
        a.is_registered_active   AS is_registered_active,
        a.is_app_active          AS is_app_active,
        a.is_gp_card_user        AS is_gp_card_user,
        a.is_heuristic_active    AS is_heuristic_active,
        a.first_action_at        AS first_action_at,
        a.last_action_at         AS last_action_at,
        a.n_actions              AS n_actions,
        -- lifecycle (real on-chain time from transaction_action.timestamp)
        dateDiff('day', a.first_action_at, a.last_action_at)                          AS tenure_days,
        dateDiff('day', a.last_action_at, now())                                      AS days_since_last_action,
        coalesce(a.last_action_at >= now() - INTERVAL 30 DAY, false)                  AS is_active_30d,
        coalesce(a.last_action_at >= now() - INTERVAL 90 DAY, false)                  AS is_active_90d,
        toStartOfMonth(a.first_action_at)                                             AS acquisition_cohort_month,
        -- engagement counts
        coalesce(sw.n_swaps_gnosis_app, 0)  AS n_swaps_gnosis_app,
        coalesce(sw.n_swaps_metri, 0)       AS n_swaps_metri,
        coalesce(tr.n_metri_transfer, 0)    AS n_metri_transfer,
        coalesce(tr.n_metri_fee, 0)         AS n_metri_fee,
        coalesce(tr.n_hub_transfer, 0)      AS n_hub_transfer,
        coalesce(tr.n_personal_mint, 0)     AS n_personal_mint,
        coalesce(tr.n_group_fee, 0)         AS n_group_fee,
        coalesce(tr.n_pay_topup, 0)         AS n_pay_topup,
        coalesce(cb.n_cashback, 0)          AS n_cashback,
        coalesce(inv.n_investment, 0)       AS n_investment,
        -- social / trust / referrals
        coalesce(ref.n_invitees, 0)         AS n_invitees,
        coalesce(ref.referral_crc_earned, 0.0) AS referral_crc_earned,
        coalesce(av.trusts_given, 0)        AS trusts_given,
        coalesce(av.trusts_received, 0)     AS trusts_received,
        coalesce(av.trusts_mutual, 0)       AS trusts_mutual,
        coalesce(av.earned_from_invites_crc, 0.0) AS earned_from_invites_crc,
        av.circles_version                  AS circles_version,
        -- breadth = number of distinct app-tagged action TYPES the wallet used
        toUInt8(a.has_swap_gnosis_io) + toUInt8(a.has_swap_metri) + toUInt8(a.has_metri_fee)
          + toUInt8(a.has_metri_transfer) + toUInt8(a.has_pay_topup) + toUInt8(a.has_cashback)
          + toUInt8(a.has_investment) + toUInt8(a.has_auto_topup)                     AS app_action_breadth
    FROM {{ ref('int_execution_gnosis_app_gt_user_activity') }} a
    LEFT JOIN tr  ON a.address = tr.address
    LEFT JOIN sw  ON a.address = sw.address
    LEFT JOIN cb  ON a.address = cb.address
    LEFT JOIN inv ON a.address = inv.address
    LEFT JOIN ref ON a.address = ref.address
    LEFT JOIN av  ON a.address = av.address
)
SELECT
    *,
    multiIf(
        NOT is_registered_active,                      'inactive',
        app_action_breadth >= 3 AND is_active_30d,     'power',
        app_action_breadth >= 2 OR  is_active_30d,     'core',
        'casual'
    )                                                  AS engagement_tier,
    (n_investment > 0)                                 AS is_investor,
    (app_action_breadth >= 3 AND is_active_30d)        AS is_power_user
FROM base
