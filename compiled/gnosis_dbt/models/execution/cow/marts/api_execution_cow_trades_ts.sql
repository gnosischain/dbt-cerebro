

SELECT
    date,
    num_trades AS value
FROM `dbt`.`fct_execution_cow_daily`
WHERE date < today()
ORDER BY date