

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_pools_lps_latest`
WHERE window = '7D'
ORDER BY token