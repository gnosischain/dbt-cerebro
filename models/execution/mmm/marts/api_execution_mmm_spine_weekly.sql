{{
  config(
    materialized='view',
    tags=['production', 'mmm', 'execution', 'tier1', 'api:mmm_spine', 'granularity:weekly']
  )
}}

-- API view passthrough over fct_execution_mmm_spine_weekly. Tier1
-- endpoint, requires X-API-Key. The MMM analyst persona reads this
-- directly to pull a complete (kpi, media, control) weekly spine
-- without hand-rolling the union.

SELECT * FROM {{ ref('fct_execution_mmm_spine_weekly') }}
ORDER BY week
