{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:execution_pools_lps_count', 'granularity:last_7d']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_pools_lps_daily') }}) AS as_of_date
FROM (
SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_pools_lps_latest') }}
WHERE window = '7D'
ORDER BY token
) AS sub
