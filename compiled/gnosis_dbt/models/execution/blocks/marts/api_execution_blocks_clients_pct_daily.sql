

SELECT
    date
    ,client
    ,pct AS value
FROM `dbt`.`fct_execution_blocks_clients_daily`
ORDER BY date, client