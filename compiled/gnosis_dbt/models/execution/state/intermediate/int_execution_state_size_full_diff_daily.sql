


WITH


state_size_diff AS (
    SELECT 
        toStartOfDay(block_timestamp) AS date 
        ,SUM(IF(to_value!='0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        `dbt`.`stg_execution__storage_diffs`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_execution_state_size_full_diff_daily`
    )
  

    GROUP BY 1
)

SELECT
    *
FROM state_size_diff