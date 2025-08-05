


WITH

state_size_diff_daily AS (
    SELECT 
        date
        ,bytes_diff
    FROM 
        `dbt`.`execution_state_size_diff_daily`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(date)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`execution_state_size_daily`
    )
  

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