


WITH


state_size_diff AS (
    SELECT 
        address
        ,block_timestamp 
        ,SUM(IF(to_value!='0x0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        `execution`.`storage_diffs`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(block_timestamp))
      FROM `dbt`.`execution_state_size_diff`
    )
  

    GROUP BY 1, 2
)

SELECT
    *
FROM state_size_diff
WHERE block_timestamp < today()