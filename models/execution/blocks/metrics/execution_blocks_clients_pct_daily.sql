{{ config(materialized='view') }}


WITH

clients_blocks AS (
    SELECT
        date
        ,client
        ,SUM(value) AS value
    FROM {{ ref('execution_blocks_clients_version_daily') }}
    GROUP BY 1, 2
)

SELECT
    date
    ,client
    ,ROUND(value/(SUM(value) OVER (PARTITION BY date)),4) AS pct
FROM 
    clients_blocks
