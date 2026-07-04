{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(event_type, first_date, user_pseudonym)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mixpanel_ga', 'gpay', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'},
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

-- INTERNAL ONLY — one row per Gnosis Pay account per first-event type, with
-- the account's Mixpanel UTM attribution attached.
--
-- event_type:
--   funded         — first time the account's Safe received an inflow
--                    (gpay_funded conversion = first Fiat Top Up / Crypto Deposit)
--   first_payment  — first card Payment by the account (min gpay_payment)
--
-- Account (counting) grain = identity_role='initial_owner' (one row per human
-- wallet per event_type). This avoids the safe_self/initial_owner double-row
-- the conversions registry emits per Safe.
--
-- UTM RESOLUTION (two paths, GA-controller wins):
--   For app-onboarded Safes the initial_owner that signed Safe setup is the
--   Cometh 4337 relayer, not the human — so the direct user_pseudonym match
--   misses and the row falls to 'unknown'. The address that actually carries
--   Mixpanel UTM is the Gnosis App user enabled on the Safe's Delay module
--   (int_execution_gnosis_app_gpay_wallets.first_ga_owner_address). We resolve
--   that GA controller's pseudonym and prefer its campaign, falling back to the
--   direct initial_owner match, then 'unknown'. Only the campaign assignment
--   changes — the account count per event_type is unchanged.
--
-- first_touch_attribution_path / last_touch_attribution_path record which
-- identity supplied each window's campaign (the two coalesces resolve
-- independently). Both are internal audit columns and are NOT propagated to
-- the weekly mart or any api_ view.
-- Carries user_pseudonym → never exposed to cerebro-api or MCP.

WITH funded AS (
    SELECT
        user_pseudonym,
        min(conversion_ts)             AS first_ts,
        argMin(gp_safe, conversion_ts) AS gp_safe,
        'funded'                       AS event_type
    FROM {{ ref('int_execution_gpay_conversions') }}
    WHERE conversion_kind = 'gpay_funded'
      AND identity_role   = 'initial_owner'
    GROUP BY user_pseudonym
),

first_payment AS (
    SELECT
        user_pseudonym,
        min(conversion_ts)             AS first_ts,
        argMin(gp_safe, conversion_ts) AS gp_safe,
        'first_payment'                AS event_type
    FROM {{ ref('int_execution_gpay_conversions') }}
    WHERE conversion_kind = 'gpay_payment'
      AND identity_role   = 'initial_owner'
    GROUP BY user_pseudonym
),

events AS (
    SELECT * FROM funded
    UNION ALL
    SELECT * FROM first_payment
),

-- App<>GP link: GP card → GA-account, via the reusable card-owner bridge
-- (int_execution_gnosis_app_gt_card_owner = Delay-module controller ∪ cashback owner ∪ top-up
-- funder, gated to registered GA accounts). Pick ONE account per card, preferring one that carries
-- a real first-touch campaign, so the touch tuple stays consistent and the account grain never
-- fans out the events. Replaces the Delay-only link (recovers ~+108 funded cards).
card_acct AS (
    SELECT
        b.card                                                             AS gp_safe,
        argMax(b.ga_account_pseudonym, a.first_touch_campaign != 'unknown') AS ga_user_pseudonym
    FROM {{ ref('int_execution_gnosis_app_gt_card_owner') }} b
    INNER JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} a
        ON a.user_id_hash = b.ga_account_pseudonym
    GROUP BY b.card
),

-- UTM for the chosen GA account (pseudonym → Mixpanel user_id_hash space).
ga_acq AS (
    SELECT
        l.gp_safe,
        a.first_touch_campaign AS ga_first_touch_campaign,
        a.last_touch_campaign  AS ga_last_touch_campaign,
        a.first_touch_source   AS ga_first_touch_source,
        a.last_touch_source    AS ga_last_touch_source,
        a.first_touch_medium   AS ga_first_touch_medium,
        a.last_touch_medium    AS ga_last_touch_medium
    FROM card_acct l
    INNER JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} a
        ON a.user_id_hash = l.ga_user_pseudonym
)

SELECT
    e.event_type                                  AS event_type,
    toDate(e.first_ts)                            AS first_date,
    e.user_pseudonym                              AS user_pseudonym,
    coalesce(
        nullIf(g.ga_first_touch_campaign, 'unknown'),
        nullIf(d.first_touch_campaign,    'unknown'),
        'unknown'
    )                                             AS first_touch_campaign,
    coalesce(
        nullIf(g.ga_last_touch_campaign, 'unknown'),
        nullIf(d.last_touch_campaign,    'unknown'),
        'unknown'
    )                                             AS last_touch_campaign,
    coalesce(
        nullIf(g.ga_first_touch_source, 'unknown'),
        nullIf(d.first_touch_source,    'unknown'),
        'unknown'
    )                                             AS first_touch_source,
    coalesce(
        nullIf(g.ga_last_touch_source, 'unknown'),
        nullIf(d.last_touch_source,    'unknown'),
        'unknown'
    )                                             AS last_touch_source,
    coalesce(
        nullIf(g.ga_first_touch_medium, 'unknown'),
        nullIf(d.first_touch_medium,    'unknown'),
        'unknown'
    )                                             AS first_touch_medium,
    coalesce(
        nullIf(g.ga_last_touch_medium, 'unknown'),
        nullIf(d.last_touch_medium,    'unknown'),
        'unknown'
    )                                             AS last_touch_medium,
    multiIf(
        nullIf(g.ga_first_touch_campaign, 'unknown') IS NOT NULL, 'ga_controller',
        nullIf(d.first_touch_campaign,    'unknown') IS NOT NULL, 'initial_owner',
        'unknown'
    )                                             AS first_touch_attribution_path,
    multiIf(
        nullIf(g.ga_last_touch_campaign, 'unknown') IS NOT NULL, 'ga_controller',
        nullIf(d.last_touch_campaign,    'unknown') IS NOT NULL, 'initial_owner',
        'unknown'
    )                                             AS last_touch_attribution_path
FROM events e
LEFT JOIN ga_acq g
    ON g.gp_safe = e.gp_safe
LEFT JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} d
    ON d.user_id_hash = e.user_pseudonym
