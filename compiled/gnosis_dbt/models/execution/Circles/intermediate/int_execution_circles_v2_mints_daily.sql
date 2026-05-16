

-- Network-level daily mint summary, broken down by mint_kind.
--   mint_kind      - 'personal' | 'group' | 'migration' | 'other'
--   n_mint_events  - number of mint TransferSingle events
--   n_minters      - distinct recipient addresses minting that day
--   amount_minted  - total CRC minted (raw / 1e18)
--
-- Source: int_execution_circles_v2_mint_events (which classifies each
-- mint via the avatar registry + V2 Hub call-decoder; see that model
-- for the classifier details). Replaces the previous logic that filtered
-- int_execution_circles_v2_hub_transfers on from-zero alone — that
-- predicate lumped personal mints, group mints, and V1→V2 migrations
-- together.




SELECT
    toDate(block_timestamp)                          AS date,
    mint_kind                                        AS mint_kind,
    count()                                          AS n_mint_events,
    uniqExact(to_address)                            AS n_minters,
    sum(toFloat64(amount_raw)) / pow(10, 18)         AS amount_minted
FROM `dbt`.`int_execution_circles_v2_mint_events`
WHERE block_timestamp < today()
  
    
  
    
    
    
    
    

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
    
  

  
GROUP BY date, mint_kind