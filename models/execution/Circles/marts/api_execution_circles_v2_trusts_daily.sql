{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_trusts', 'granularity:daily']
  )
}}

SELECT
    date,
    n_trust_events,
    n_new_trusts,
    n_revoked_trusts,
    n_distinct_trusters,
    n_distinct_trustees
FROM {{ ref('fct_execution_circles_v2_trusts_daily') }}
WHERE date < today()
ORDER BY date DESC
