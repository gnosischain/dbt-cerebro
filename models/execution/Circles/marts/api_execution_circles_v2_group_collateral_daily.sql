{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_collateral','granularity:daily']
    )
}}

-- Daily end-of-day member-CRC collateral held by each Circles v2 group
-- (native units), summed across all backing token ids. Latest incomplete
-- day excluded per api_ convention.
SELECT
    date,
    group_address,
    sum(balance_raw) / 1e18 AS collateral
FROM (
    SELECT date, lower(group_address) AS group_address, balance_raw
    FROM {{ ref('int_execution_circles_v2_group_collateral_balances_daily') }}
    WHERE date < today()
)
GROUP BY date, group_address
