


WITH


blocks_extra_data AS (
    SELECT 
        block_timestamp
        ,extra_data
    FROM 
        `execution`.`blocks`
    WHERE 
        block_timestamp > '1970-01-01' -- remove genesis
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(block_timestamp))
      FROM `dbt`.`execution_blocks_production`
    )
  

)

SELECT
    *
FROM blocks_extra_data
WHERE block_timestamp < today()