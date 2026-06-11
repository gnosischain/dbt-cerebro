{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(conversion_kind, first_date, user_pseudonym)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mixpanel_ga', 'gnosis_app', 'internal_only', 'privacy:tier_internal'],
    meta={'expose_to_mcp': False, 'privacy_tier': 'internal'},
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

-- INTERNAL ONLY — one row per Gnosis App account per first-conversion type,
-- with the account's Mixpanel UTM attribution attached.
--
-- Unlike the GP side (which barely overlaps Mixpanel), Gnosis App conversions
-- match the Mixpanel identified-user set 70-83% — because both describe the
-- same app.gnosis.io population, keyed on the same on-chain identity bridge.
--
-- conversion_kind ∈ {topup, swap_filled, token_offer_claim, marketplace_buy,
-- starts_referring}. 'topup' is a GP card funding initiated from inside the
-- app — the closest UTM-attributable "first funded" signal.
-- 'starts_referring' is the on-chain referral milestone: first time the
-- user's address appears as invited_by on a new Circles Human registration
-- (int_execution_circles_v2_referrers).
--
-- UTM attached via user_pseudonym == user_id_hash. Carries user_pseudonym →
-- never exposed to cerebro-api or MCP.

WITH first_conv AS (
    SELECT
        user_pseudonym,
        conversion_kind,
        min(conversion_ts) AS first_ts
    FROM {{ ref('int_execution_gnosis_app_conversions') }}
    GROUP BY user_pseudonym, conversion_kind

    UNION ALL

    SELECT
        {{ pseudonymize_address('inviter') }}  AS user_pseudonym,
        'starts_referring'                     AS conversion_kind,
        first_referral_at                      AS first_ts
    FROM {{ ref('int_execution_circles_v2_referrers') }}
)

SELECT
    f.conversion_kind                             AS conversion_kind,
    toDate(f.first_ts)                            AS first_date,
    f.user_pseudonym                              AS user_pseudonym,
    coalesce(a.first_touch_campaign, 'unknown')   AS first_touch_campaign,
    coalesce(a.last_touch_campaign,  'unknown')   AS last_touch_campaign,
    coalesce(a.first_touch_source,   'unknown')   AS first_touch_source,
    coalesce(a.last_touch_source,    'unknown')   AS last_touch_source,
    coalesce(a.first_touch_medium,   'unknown')   AS first_touch_medium,
    coalesce(a.last_touch_medium,    'unknown')   AS last_touch_medium
FROM first_conv f
LEFT JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} a
    ON a.user_id_hash = f.user_pseudonym
