{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'yields', 'api:yields_overview', 'granularity:latest']
    )
}}

SELECT
    value,
    change_pct,
    label
FROM {{ ref('fct_execution_yields_overview_snapshot') }}
WHERE metric = 'sdai_apy'
