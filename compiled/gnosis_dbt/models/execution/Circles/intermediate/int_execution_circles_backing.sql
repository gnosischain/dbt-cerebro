


SELECT
    toStartOfDay(block_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`
WHERE 
    event_name = 'CirclesBackingCompleted'
    
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_circles_backing` AS x1
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_circles_backing` AS x2
    )
  

GROUP BY 1