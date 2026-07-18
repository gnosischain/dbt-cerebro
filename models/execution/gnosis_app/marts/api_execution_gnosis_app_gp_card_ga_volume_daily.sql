{{
  config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app', 'gpay', 'granularity:daily'],
    meta={
      'owner': 'analytics_team',
      'authoritative': false,
      'api': {'exclude_from_api': true}
    }
  )
}}

-- Dashboard view of fct_execution_gnosis_app_gp_card_ga_volume_daily: GA-linked GP-card funding & spend
-- per day, split by link_source, with cumulative series. Aggregate sums/counts only — no address/pseudonym.
-- DELIBERATELY NOT exposed to cerebro-api (no `api:` tag + api.exclude_from_api): the series is partly
-- Mixpanel-derived, so it is served only via the metrics-dashboard x-api-key SQL layer — the same exposure
-- path as api_execution_gnosis_app_gp_card_ga_link_daily and the existing Mixpanel campaign cards.

SELECT
    date,
    link_source,
    funded_volume_usd,
    spend_usd,
    spend_count,
    spending_cards,
    funded_volume_cumulative_usd,
    spend_cumulative_usd
FROM {{ ref('fct_execution_gnosis_app_gp_card_ga_volume_daily') }}
WHERE date < today()   -- exclude the current, incomplete day
ORDER BY date, link_source
