

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_pools_lps_daily`) AS as_of_date
FROM (
SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_pools_lps_latest`
WHERE window = '7D'
ORDER BY token
) AS sub