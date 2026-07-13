{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:execution_pools_volume_usd', 'metric:volume_7d', 'granularity:snapshot', 'window:7d']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('fct_execution_pools_daily') }}) AS as_of_date
FROM (
SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_pools_snapshots') }}
WHERE metric = 'Volume_7D'
ORDER BY value DESC
) AS sub
