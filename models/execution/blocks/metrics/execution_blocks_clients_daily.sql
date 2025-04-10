{{ config(materialized='view') }}


SELECT
    date
    ,client
    ,SUM(value) AS value
FROM {{ ref('execution_blocks_clients_version_daily') }}
GROUP BY 1, 2

