{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier0', 'api:circles_v2_kpi_new_backers', 'granularity:latest', 'window:7d']
    )
}}

-- KPI tile: backers newly trusted by the backers group in the last 7 days,
-- with week-over-week change.

WITH windowed AS (
    SELECT
        sumIf(new_backers, date >  today() - 7 AND date <= today())   AS value,
        sumIf(new_backers, date >  today() - 14 AND date <= today() - 7) AS prior_value
    FROM {{ ref('fct_execution_circles_v2_backers_cumulative_daily') }}
    WHERE date > today() - 14
)

SELECT
    value                                                            AS value,
    round((value - prior_value) / nullIf(prior_value, 0) * 100, 1)   AS change_pct
FROM windowed
