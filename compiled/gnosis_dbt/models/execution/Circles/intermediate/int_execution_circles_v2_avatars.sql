



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
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_circles_v2_avatars` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_circles_v2_avatars` AS t2
    )
  

GROUP BY 1,2