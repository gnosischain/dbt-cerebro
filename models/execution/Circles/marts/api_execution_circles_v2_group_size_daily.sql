{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_size','granularity:daily']
    )
}}

-- Dashboard view over the per-group daily member count. Latest incomplete
-- day excluded per api_ convention.
SELECT date, group_address, n_members
FROM {{ ref('int_execution_circles_v2_group_size_daily') }}
WHERE date < today()
