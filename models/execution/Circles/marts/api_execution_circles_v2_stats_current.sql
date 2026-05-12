{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_stats_current','granularity:latest']
  )
}}

-- Snapshot of network-level Circles v2 counts: avatars (total + by type),
-- active trusts, tokens, wrappers. Thin passthrough over
-- fct_execution_circles_v2_stats_current.

SELECT
    measure,
    value
FROM {{ ref('fct_execution_circles_v2_stats_current') }}
ORDER BY measure
