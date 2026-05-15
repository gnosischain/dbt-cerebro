

-- Network-level daily personal-mint summary.
--   n_mint_events  - number of mint TransferSingle events
--   n_minters      - distinct avatars minting that day
--   amount_minted  - total CRC minted (raw / 1e18)
--
-- Source: int_execution_circles_v2_hub_transfers filtered to mint events
-- (from_address = 0x00…00, to_address = recipient avatar). Mirrors the
-- semantics of api_execution_circles_v2_avatar_mint_activity_daily but
-- collapsed to a single row per day.




SELECT
    toDate(block_timestamp)                          AS date,
    count()                                          AS n_mint_events,
    uniqExact(to_address)                            AS n_minters,
    sum(toFloat64(amount_raw)) / pow(10, 18)         AS amount_minted
FROM `dbt`.`int_execution_circles_v2_hub_transfers`
WHERE from_address = '0x0000000000000000000000000000000000000000'
  AND to_address  != '0x0000000000000000000000000000000000000000'
  AND block_timestamp < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_mints_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_mints_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date