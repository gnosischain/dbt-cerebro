

SELECT 
    round(cum_net_usd, 2) AS value
FROM `dbt`.`fct_bridges_kpis_snapshot`
ORDER BY as_of_date DESC
LIMIT 1