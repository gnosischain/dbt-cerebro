{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier0', 'api:circles_v2_kpi_avg_members_per_group', 'granularity:latest']
    )
}}

-- KPI tile: average and median member count across Circles v2 groups.
-- Static snapshot (no time series upstream for size yet), so change_pct
-- is left NULL — downstream KPI tile renders without a delta arrow.

SELECT
    round(avg(n_members), 1)               AS value,
    toFloat64(median(n_members))           AS median_members,
    toFloat64(NULL)                        AS change_pct
FROM {{ ref('fct_execution_circles_v2_group_size_current') }}
