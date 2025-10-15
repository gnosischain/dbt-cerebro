

SELECT
    date
    ,client
    ,ROUND(fraq * 100, 2) AS value
FROM `dbt`.`fct_execution_blocks_clients_daily`
ORDER BY date, client