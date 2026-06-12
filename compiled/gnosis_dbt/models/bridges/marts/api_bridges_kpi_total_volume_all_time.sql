

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_bridges_flows_daily`) AS as_of_date
FROM (
SELECT 
    round(cum_vol_usd, 2) AS value
FROM `dbt`.`fct_bridges_kpis_snapshot`
ORDER BY as_of_date DESC
LIMIT 1
) AS sub