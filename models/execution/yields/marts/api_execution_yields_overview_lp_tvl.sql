{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_overview', 'granularity:latest', 'tier1']
    )
}}

SELECT
    value,
    change_pct,
    label
FROM {{ ref('fct_execution_yields_overview_snapshot') }}
WHERE metric = 'lp_tvl_total'
