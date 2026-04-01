{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:execution_pools', 'metric:volume_7d', 'granularity:snapshot']
    )
}}

SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_pools_snapshots') }}
WHERE metric = 'Volume_7D'
ORDER BY value DESC
