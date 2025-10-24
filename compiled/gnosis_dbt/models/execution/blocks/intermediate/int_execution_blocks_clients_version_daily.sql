

WITH

clients_version AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,multiIf(
             lower(decoded_extra_data[1]) = 'choose' 
            OR lower(decoded_extra_data[1]) = 'mysticryuujin'  
            OR lower(decoded_extra_data[1]) = 'sanae.io'
            OR decoded_extra_data[1] = ''  , 
            'Unknown',
            decoded_extra_data[1]
        )   AS client
        ,IF(length(decoded_extra_data)>1, 
            IF(decoded_extra_data[2]='Ethereum',decoded_extra_data[3],decoded_extra_data[2]), 
            ''
        ) AS version
        ,COUNT(*) AS cnt
    FROM `dbt`.`stg_execution__blocks`
    
  
    
      
    

   WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_blocks_clients_version_daily` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_blocks_clients_version_daily` AS t2
    )
  

    GROUP BY 1, 2, 3
)

SELECT
    *
FROM clients_version