{{
  config(
    materialized='view',
    tags=['production','execution','tier0','api:circles_v2_kpi_avg_trusts_per_avatar','granularity:latest']
  )
}}

-- KPI tile: average trusts per human avatar = active_trusts / humans.
-- A network-density indicator. Derived from fct_execution_circles_v2_stats_current.

WITH s AS (
    SELECT measure, value
    FROM {{ ref('fct_execution_circles_v2_stats_current') }}
    WHERE measure IN ('active_trust_count_v2', 'human_count_v2')
)

SELECT
    round(
        toFloat64(anyIf(value, measure = 'active_trust_count_v2'))
        / nullIf(toFloat64(anyIf(value, measure = 'human_count_v2')), 0),
        2
    ) AS value
FROM s
