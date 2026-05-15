

-- Daily Circles v2 backing-lifecycle event counts. Tracks the "depositors" set —
-- addresses that emit a backing event. This is the *transactional* population
-- (not the trust-defined "backers" set, which awaits the backers-group address).
--
--   n_events           - total events in this stage on this day
--   n_distinct_backers - distinct `backer` addresses
--   n_distinct_assets  - distinct backing assets pledged




SELECT
    toDate(block_timestamp)                     AS date,
    lifecycle_stage                             AS lifecycle_stage,
    count()                                     AS n_events,
    uniqExactIf(backer, backer IS NOT NULL)     AS n_distinct_backers,
    uniqExactIf(backing_asset, backing_asset IS NOT NULL) AS n_distinct_assets
FROM `dbt`.`int_execution_circles_v2_backing`
WHERE block_timestamp < today()
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_backing_events_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_backing_events_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, lifecycle_stage