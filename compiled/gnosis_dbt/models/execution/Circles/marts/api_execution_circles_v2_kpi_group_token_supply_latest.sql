

-- KPI tile: total Circles v2 group-token supply (aggregate across all
-- groups, summed across native ERC-1155 and ERC-20 wrappers), with WoW
-- change derived from the daily aggregate.

WITH current AS (
    SELECT supply_total AS value
    FROM `dbt`.`fct_execution_circles_v2_group_token_supply_daily`
    WHERE date = (
        SELECT max(date)
        FROM `dbt`.`fct_execution_circles_v2_group_token_supply_daily`
        WHERE date < today()
    )
),
prior AS (
    SELECT supply_total AS value
    FROM `dbt`.`fct_execution_circles_v2_group_token_supply_daily`
    WHERE date = (
        SELECT max(date)
        FROM `dbt`.`fct_execution_circles_v2_group_token_supply_daily`
        WHERE date < today()
    ) - 7
)

SELECT
    c.value                                                            AS value,
    round((c.value - p.value) / nullIf(p.value, 0) * 100, 1)           AS change_pct
FROM current c
CROSS JOIN prior p