



SELECT
    block_number,
    block_timestamp,
    lower(CONCAT('0x', transaction_hash)) AS transaction_hash,
    transaction_index,
    log_index,
    CASE
        WHEN event_name = 'Signup' THEN 'Human'
        WHEN event_name = 'OrganizationSignup' THEN 'Org'
        ELSE 'Unknown'
    END AS avatar_type,
    lower(
        CASE
            WHEN event_name = 'Signup' THEN decoded_params['user']
            ELSE decoded_params['organization']
        END
    ) AS avatar,
    lower(
        CASE
            WHEN event_name = 'Signup' THEN decoded_params['token']
            ELSE NULL
        END
    ) AS token_id
FROM `dbt`.`contracts_circles_v1_Hub_events`
WHERE
    event_name IN ('Signup', 'OrganizationSignup')
    
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_circles_v1_avatars` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_circles_v1_avatars` AS x2
      WHERE 1=1 
    )
  

    