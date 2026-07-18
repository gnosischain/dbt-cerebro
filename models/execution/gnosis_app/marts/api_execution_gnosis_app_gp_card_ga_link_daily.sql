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

-- Dashboard-facing view of fct_execution_gnosis_app_gp_card_ga_link_daily.
-- Aggregate counts only (date, link_source, new, cumulative) — no address/pseudonym.
-- DELIBERATELY NOT exposed to cerebro-api (no `api:` tag + api.exclude_from_api): this series is
-- partly Mixpanel-derived, so it is served only through the metrics-dashboard x-api-key SQL layer
-- (the dashboard card queries `FROM dbt.api_execution_gnosis_app_gp_card_ga_link_daily` directly),
-- the same exposure path as the existing Mixpanel campaign cards.

SELECT
    date,
    link_source,
    n_cards_new,
    n_cards_cumulative
FROM {{ ref('fct_execution_gnosis_app_gp_card_ga_link_daily') }}
WHERE date < today()   -- exclude the current, incomplete day
ORDER BY date, link_source
