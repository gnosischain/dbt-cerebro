



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
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_execution_circles_v2_avatars`
    )
  

GROUP BY 1,2