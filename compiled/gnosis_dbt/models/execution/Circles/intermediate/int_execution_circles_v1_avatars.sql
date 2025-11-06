


SELECT
    block_timestamp
    ,decoded_params['avatar'] AS user_address
    ,decoded_params['inviter'] AS inviter_address
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE 
    event_name = 'RegisterHuman'
    
  
    
      
    

   WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS x1
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS x2
    )
  
