{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_backing_depositors_current','granularity:latest']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_circles_v2_backing') }}) AS as_of_date
FROM (
SELECT
    backer,
    first_initiated_at,
    last_event_at,
    n_initiated,
    n_completed,
    n_released,
    n_distinct_assets
FROM {{ ref('int_execution_circles_v2_backing_depositors_current') }}
ORDER BY first_initiated_at
) AS sub
