


WITH


state_size_diff AS (
    SELECT 
        address
        ,toStartOfDay(block_timestamp) AS date 
        ,SUM(IF(to_value!='0x0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        `execution`.`storage_diffs`
    WHERE
        block_timestamp < today()
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(block_timestamp))
      FROM `dbt`.`execution_state_size_diff_daily`
    )
  

    GROUP BY 1, 2
)

SELECT
    *
FROM state_size_diff