


SELECT
    toStartOfDay(block_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`
WHERE 
    event_name = 'CirclesBackingCompleted'
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_execution_circles_backing`
    )
  

GROUP BY 1