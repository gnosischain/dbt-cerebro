



SELECT
    toStartOfDay(block_timestamp) AS date
    ,CASE  
        WHEN event_name = 'RegisterHuman' THEN 'Human' 
        WHEN event_name = 'RegisterGroup' THEN 'Group' 
        WHEN event_name = 'RegisterOrganization' THEN 'Org'
        ELSE 'Unknown' 
    END AS avatar_type
    ,COUNT(*) AS cnt
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE 
    event_name IN ('RegisterHuman','RegisterGroup','RegisterOrganization')
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_circles_v2_avatars` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_circles_v2_avatars` AS x2
      WHERE 1=1 
    )
  

GROUP BY 1,2