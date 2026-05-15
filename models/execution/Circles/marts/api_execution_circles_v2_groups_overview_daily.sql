{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_groups_overview','granularity:daily']
  )
}}

SELECT
    date,
    n_new_groups,
    n_collateral_events,
    n_distinct_groups_acting,
    sum(n_new_groups) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_groups_total
FROM {{ ref('int_execution_circles_v2_groups_overview_daily') }}
WHERE date < today()
ORDER BY date DESC
