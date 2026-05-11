{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_hub_events', 'granularity:daily']
  )
}}

SELECT
    date,
    event_name,
    n_events,
    n_tx,
    n_distinct_addresses
FROM {{ ref('fct_execution_circles_v2_hub_events_daily') }}
WHERE date < today()
ORDER BY date DESC, event_name
