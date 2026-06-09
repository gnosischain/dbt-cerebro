{{
  config(
    materialized='view',
    tags=['production', 'mta', 'execution', 'gpay', 'tier1', 'api:gpay_attribution', 'granularity:rolling_180d', 'window:60d']
  )
}}

-- API view passthrough over fct_execution_gpay_attribution_60d.
-- Tier1 endpoint, requires X-API-Key. The `identity_role` column lets
-- callers filter to owner-grain (`initial_owner`), treasury-grain
-- (`safe_self`), or delegate-grain at query time.

SELECT
  conversion_kind,
  identity_role,
  event_kind,
  conversions_with_touch,
  first_touch,
  last_touch,
  linear,
  time_decay_hl_7d,
  total_conversions,
  computed_at
FROM {{ ref('fct_execution_gpay_attribution_60d') }}
ORDER BY conversion_kind, identity_role, linear DESC
