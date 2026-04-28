



SELECT
    block_number,
    block_timestamp,
    lower(concat('0x', transaction_hash)) AS transaction_hash,
    transaction_index,
    log_index,
    CASE
        WHEN event_name = 'RegisterHuman' THEN 'Human'
        WHEN event_name = 'RegisterGroup' THEN 'Group'
        WHEN event_name = 'RegisterOrganization' THEN 'Org'
        ELSE 'Unknown'
    END AS avatar_type,
    lower(
        CASE
            WHEN event_name = 'RegisterHuman' THEN decoded_params['inviter']
            ELSE NULL
        END
    ) AS invited_by,
    lower(
        CASE
            WHEN event_name = 'RegisterHuman' THEN decoded_params['avatar']
            WHEN event_name = 'RegisterGroup' THEN decoded_params['group']
            ELSE decoded_params['organization']
        END
    ) AS avatar,
    lower(
        CASE
            WHEN event_name = 'RegisterHuman' THEN decoded_params['avatar']
            WHEN event_name = 'RegisterGroup' THEN decoded_params['group']
            ELSE NULL
        END
    ) AS token_id,
    CASE
        WHEN event_name IN ('RegisterGroup', 'RegisterOrganization') THEN decoded_params['name']
        ELSE NULL
    END AS name
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE
    event_name IN ('RegisterHuman','RegisterGroup','RegisterOrganization')
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_avatars` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.block_timestamp)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_avatars` AS x2
        WHERE 1=1 
      )
    
  

    