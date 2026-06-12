{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_overview', 'granularity:latest', 'tier1']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('fct_execution_pools_daily') }}) AS as_of_date
FROM (
SELECT
    value,
    change_pct,
    label
FROM {{ ref('fct_execution_yields_overview_snapshot') }}
WHERE metric = 'lending_lenders_total'
) AS sub
