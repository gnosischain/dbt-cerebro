{{ config(materialized='view', tags=['production','execution','circles_v2','api:circles_v2_pool_search','granularity:snapshot','tier1']) }}
-- (pool_address, display_name) lookup that backs the Pool Explorer filter dropdown.
SELECT today() AS as_of_date, lower(pool_address) AS pool_address, label AS display_name
FROM {{ ref('circles_liquidity_pools') }}
