


SELECT
    block_timestamp
    ,decoded_params['avatar'] AS user_address
    ,decoded_params['inviter'] AS inviter_address
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE 
    event_name = 'RegisterHuman'
    
  
