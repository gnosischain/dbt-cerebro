


WITH

state_size_diff_daily AS (
    SELECT 
        toStartOfDay(block_timestamp) AS date
        ,SUM(bytes_diff) AS bytes_diff
    FROM 
        `dbt`.`execution_state_size_diff`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`execution_state_size_daily`
    )
  

    GROUP BY 1
),


last_partition_value AS (
    SELECT 
        bytes
    FROM 
        `dbt`.`execution_state_size_daily`
    WHERE
        toStartOfMonth(date) = (
            SELECT addMonths(max(toStartOfMonth(date)), -1)
            FROM `dbt`.`execution_state_size_daily`
        )
    ORDER BY date DESC
    LIMIT 1
),


final AS (
    SELECT
        date
        ,SUM(bytes_diff) OVER (ORDER BY date ASC) 
        
            + (SELECT bytes FROM last_partition_value)
        
        AS bytes
    FROM state_size_diff_daily
)

SELECT * FROM final