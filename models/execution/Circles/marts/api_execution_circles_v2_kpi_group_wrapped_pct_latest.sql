{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier0', 'api:circles_v2_kpi_group_wrapped_pct', 'granularity:latest']
    )
}}

-- KPI tile: share of aggregate Circles v2 group-token supply currently held
-- as ERC-20 wrappers (versus native ERC-1155). 7-day pct-point delta vs.
-- the same point a week ago.

WITH current_date_d AS (
    SELECT max(date) AS d
    FROM {{ ref('fct_execution_circles_v2_group_token_supply_daily') }}
    WHERE date < today()
),
current AS (
    SELECT
        round(supply_wrapped_erc20 / nullIf(supply_total, 0) * 100, 2) AS value
    FROM {{ ref('fct_execution_circles_v2_group_token_supply_daily') }}
    WHERE date = (SELECT d FROM current_date_d)
),
prior AS (
    SELECT
        round(supply_wrapped_erc20 / nullIf(supply_total, 0) * 100, 2) AS value
    FROM {{ ref('fct_execution_circles_v2_group_token_supply_daily') }}
    WHERE date = (SELECT d FROM current_date_d) - 7
)

SELECT
    c.value                                AS value,
    round(c.value - p.value, 2)            AS change_pct
FROM current c
CROSS JOIN prior p
