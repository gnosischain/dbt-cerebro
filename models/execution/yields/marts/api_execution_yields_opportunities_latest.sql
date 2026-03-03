{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'yields', 'api:yields_overview', 'granularity:latest']
    )
}}

SELECT
    type,
    token,
    name,
    yield_pct,
    yield_label,
    borrow_apy,
    tvl,
    fees_7d,
    protocol
FROM {{ ref('fct_execution_yields_opportunities_latest') }}
ORDER BY yield_pct DESC
