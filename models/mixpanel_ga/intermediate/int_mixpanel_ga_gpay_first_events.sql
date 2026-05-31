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
-- Account grain = identity_role='initial_owner' (the human-wallet identity
-- Mixpanel identify() keys on). This avoids the safe_self/initial_owner
-- double-row the conversions registry emits per Safe.
--
-- UTM is attached via user_pseudonym == user_id_hash (same salted
-- pseudonymize_address macro on both sides). Accounts with no Mixpanel match
-- fall to 'unknown' so totals reconcile to the full GP funnel.
-- Carries user_pseudonym → never exposed to cerebro-api or MCP.

WITH funded AS (
    SELECT
        user_pseudonym,
        min(conversion_ts) AS first_ts,
        'funded'           AS event_type
    FROM {{ ref('int_execution_gpay_conversions') }}
    WHERE conversion_kind = 'gpay_funded'
      AND identity_role   = 'initial_owner'
    GROUP BY user_pseudonym
),

first_payment AS (
    SELECT
        user_pseudonym,
        min(conversion_ts) AS first_ts,
        'first_payment'    AS event_type
    FROM {{ ref('int_execution_gpay_conversions') }}
    WHERE conversion_kind = 'gpay_payment'
      AND identity_role   = 'initial_owner'
    GROUP BY user_pseudonym
),

events AS (
    SELECT * FROM funded
    UNION ALL
    SELECT * FROM first_payment
)

SELECT
    e.event_type                                  AS event_type,
    toDate(e.first_ts)                            AS first_date,
    e.user_pseudonym                              AS user_pseudonym,
    coalesce(a.first_touch_campaign, 'unknown')   AS first_touch_campaign,
    coalesce(a.last_touch_campaign,  'unknown')   AS last_touch_campaign
FROM events e
LEFT JOIN {{ ref('int_mixpanel_ga_user_acquisition') }} a
    ON a.user_id_hash = e.user_pseudonym
