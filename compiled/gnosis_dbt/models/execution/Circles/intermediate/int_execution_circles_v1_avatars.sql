


SELECT
    block_timestamp
    ,decoded_params['avatar'] AS user_address
    ,decoded_params['inviter'] AS inviter_address
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE 
    event_name = 'RegisterHuman'
    
  
    
    

   WHERE 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_circles_v1_avatars` AS x2
      WHERE 1=1 
    )
  
