


SELECT
    block_timestamp
    ,decoded_params['avatar'] AS user_address
    ,decoded_params['inviter'] AS inviter_address
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE 
    event_name = 'RegisterHuman'
    
  
    
      
    

   WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS t2
    )
  
