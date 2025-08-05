

SELECT
    address
    ,SUM(bytes_diff) AS bytes
FROM 
    `dbt`.`execution_state_size_diff_daily`
GROUP BY 1