{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'gpay', 'granularity:weekly']
  )
}}

-- GROWTH — Gnosis Pay funded / first_payment as WEEKLY TOTALS (no campaign dimension).
-- GP-side conversions (Safe inflow / first card payment) are keyed to the Safe owner
-- (Cometh relayer / exchange / GP-direct cardholder), which Mixpanel (app.gnosis.io only)
-- never sees, so campaign attribution is structurally ~0.2% and 26-82% back-leaked. Rather
-- than publish a near-empty campaign split, expose the honest uncut totals; the attributable
-- funding signal is the app-side 'topup' in api_mixpanel_ga_growth_campaign_weekly.
-- Aggregate-only; summed over campaign within ONE attribution window (per-week totals are
-- identical across windows, so pick first_touch to avoid the stacked double-count).

SELECT
    week,
    event_type,
    sum(new_accounts) AS new_accounts
FROM {{ ref('api_mixpanel_ga_gpay_acquisition_weekly') }}
WHERE attribution_model = 'first_touch'
GROUP BY week, event_type
