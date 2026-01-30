{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'tier1', 'api:yields_pools', 'metric:fees_7d', 'granularity:snapshot']
    )
}}

SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_yields_pools_snapshots') }}
WHERE metric = 'Fees_7D'
ORDER BY value DESC
