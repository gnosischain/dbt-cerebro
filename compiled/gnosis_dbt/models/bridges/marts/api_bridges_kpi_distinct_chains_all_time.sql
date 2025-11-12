
SELECT distinct_chains AS value
FROM `dbt`.`fct_bridges_kpis_snapshot`
ORDER BY as_of_date DESC
LIMIT 1