

SELECT
    date,
    volume_usd AS value
FROM `dbt`.`fct_execution_cow_daily`
WHERE date < today()
ORDER BY date