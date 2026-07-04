{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'gnosis_app', 'granularity:weekly']
  )
}}

-- GROWTH — the campaign-attributable Gnosis App conversion funnel by UTM campaign x week.
-- Domenico-facing. Aggregate-only projection of api_mixpanel_ga_gnosis_app_acquisition_weekly
-- with the D0 causal-validity treatment applied end to end:
--   * first_touch window only (single attribution stack, no double count),
--   * clipped to week >= 2025-10-01 (the Mixpanel era) — before this, every "attributed"
--     conversion is back-stamped: the campaign touch necessarily post-dates the conversion,
--   * touch-precedence gate baked into the base model int_mixpanel_ga_gnosis_app_first_events
--     (a campaign is credited only when its touch <= conversion ts; touch-after -> 'unknown').
-- conversion_kind: topup = app card top-up (the attributable "funded" proxy),
--   starts_referring = first on-chain referral milestone, swap_filled, token_offer_claim.
-- HONEST CEILING: even here only ~4-7% of in-era app conversions carry a causally-valid
-- campaign; the rest are organic / untagged (utm_campaign='unknown'). GP-side funded /
-- first_payment are structurally un-attributable (~0.2%) and are exposed as TOTALS instead
-- (api_mixpanel_ga_gpay_funded_totals_weekly). cerebro-api exposure is blanket-excluded for
-- models/mixpanel_ga/; this is served via the metrics-dashboard x-api-key layer.

SELECT
    week,
    conversion_kind,
    utm_campaign,
    utm_source,
    utm_medium,
    new_accounts
FROM {{ ref('api_mixpanel_ga_gnosis_app_acquisition_weekly') }}
WHERE attribution_model = 'first_touch'
  AND week >= '2025-10-01'
  AND conversion_kind IN ('topup', 'starts_referring', 'swap_filled', 'token_offer_claim')
