

-- API view passthrough over fct_execution_gnosis_app_attribution_30d.
-- Tier1 endpoint, requires X-API-Key.

SELECT
  conversion_kind,
  event_kind,
  conversions_with_touch,
  first_touch,
  last_touch,
  linear,
  time_decay_hl_7d,
  total_conversions,
  computed_at
FROM `dbt`.`fct_execution_gnosis_app_attribution_30d`
ORDER BY conversion_kind, linear DESC