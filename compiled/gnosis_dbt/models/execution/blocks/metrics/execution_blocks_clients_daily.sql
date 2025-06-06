


SELECT
    date
    ,client
    ,SUM(value) AS value
FROM `dbt`.`execution_blocks_clients_version_daily`
GROUP BY 1, 2