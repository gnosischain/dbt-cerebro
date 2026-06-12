{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_stats_current','granularity:latest']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_circles_v2_avatars') }}) AS as_of_date
FROM (
-- Snapshot of network-level Circles v2 counts: avatars (total + by type),
-- active trusts, tokens, wrappers. Thin passthrough over
-- fct_execution_circles_v2_stats_current.

SELECT
    measure,
    value
FROM {{ ref('fct_execution_circles_v2_stats_current') }}
ORDER BY measure
) AS sub
