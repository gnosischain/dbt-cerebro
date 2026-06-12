

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`) AS as_of_date
FROM (
-- KPI tile: latest network-wide CRC supply with 7-day pct change.

WITH current AS (
    SELECT total_supply AS value
    FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`
    WHERE date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_total_supply_daily` WHERE date < today())
),
prior AS (
    SELECT total_supply AS value
    FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`
    WHERE date = (SELECT max(date) FROM `dbt`.`fct_execution_circles_v2_total_supply_daily` WHERE date < today()) - 7
)

SELECT
    c.value AS value,
    round((c.value - p.value) / nullIf(p.value, 0) * 100, 1) AS change_pct
FROM current c
CROSS JOIN prior p
) AS sub