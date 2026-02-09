{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'tier1', 'api:yields_pools', 'metric:tvl_latest', 'granularity:snapshot']
    )
}}


SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_yields_pools_snapshots') }}
WHERE metric = 'TVL_Latest'
ORDER BY value DESC
