


WITH

clients_blocks AS (
    SELECT
        date
        ,client
        ,SUM(cnt) AS cnt
    FROM `dbt`.`int_execution_blocks_clients_version_daily`
    WHERE date < today()
    GROUP BY 1, 2
)

SELECT
    date
    ,client
    ,cnt
    ,ROUND(cnt/(SUM(cnt) OVER (PARTITION BY date)),4) AS pct
FROM 
    clients_blocks