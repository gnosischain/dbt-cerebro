{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'yields', 'api:yields_overview', 'granularity:latest']
    )
}}

SELECT
    type,
    name,
    yield_pct,
    borrow_apy,
    tvl,
    protocol
FROM {{ ref('fct_execution_yields_opportunities_latest') }}
ORDER BY yield_pct DESC
