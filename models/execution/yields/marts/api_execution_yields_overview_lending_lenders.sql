{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_overview', 'granularity:latest']
    )
}}

SELECT
    value,
    change_pct,
    label
FROM {{ ref('fct_execution_yields_overview_snapshot') }}
WHERE metric = 'lending_lenders_total'
