{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:execution_pools_tvl_usd', 'metric:tvl_latest', 'granularity:snapshot']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('fct_execution_pools_daily') }}) AS as_of_date
FROM (
SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_pools_snapshots') }}
WHERE metric = 'TVL_Latest'
ORDER BY value DESC
) AS sub
