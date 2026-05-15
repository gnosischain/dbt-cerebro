

-- Event-grain daily trust activity (complements fct_execution_circles_v2_active_trusts_daily
-- which carries the SCD2-derived net active stock from int_execution_circles_v2_trust_pair_ranges).
--
--   n_trust_events     - total Trust events on this day
--   n_new_trusts       - events with expiry > block_timestamp (trust granted/extended)
--   n_revoked_trusts   - events with expiry <= block_timestamp (set to 0 = revoke)
--   n_distinct_trusters - distinct truster addresses active that day
--   n_distinct_trustees - distinct trustee addresses active that day




SELECT
    toDate(block_timestamp)                                              AS date,
    count()                                                              AS n_trust_events,
    countIf(expiry_time >  block_timestamp)                              AS n_new_trusts,
    countIf(expiry_time <= block_timestamp)                              AS n_revoked_trusts,
    uniqExact(truster)                                                   AS n_distinct_trusters,
    uniqExact(trustee)                                                   AS n_distinct_trustees
FROM `dbt`.`int_execution_circles_v2_trust_updates`
WHERE block_timestamp < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_trusts_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_trusts_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date