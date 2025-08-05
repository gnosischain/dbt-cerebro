{{ config(materialized='view') }}

SELECT
    address
    ,SUM(bytes_diff) AS bytes
FROM 
    {{ ref('execution_state_size_diff_daily') }}
GROUP BY 1