


SELECT
    toStartOfDay(block_timestamp) AS date
    ,COUNT(*) AS cnt
FROM `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`
WHERE 
    event_name = 'CirclesBackingCompleted'
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_circles_backing` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_circles_backing` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1