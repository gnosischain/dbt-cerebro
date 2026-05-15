

-- Network-level daily group metrics:
--   n_new_groups          - groups registered that day (RegisterGroup events)
--   n_groups_total        - cumulative group count up to and including this day
--   n_collateral_events   - StandardTreasury lock/burn/return events on this day
--   n_distinct_groups_acting - distinct groups touching collateral that day
--
-- Sources:
--   * int_execution_circles_v2_avatars (avatar_type = 'Group') for the
--     registration stream.
--   * int_execution_circles_v2_group_collateral_diffs for treasury events.




WITH new_groups AS (
    SELECT
        toDate(block_timestamp) AS date,
        count()                 AS n_new_groups
    FROM `dbt`.`int_execution_circles_v2_avatars`
    WHERE avatar_type = 'Group'
      AND block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_groups_overview_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_groups_overview_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY date
),

collateral AS (
    SELECT
        toDate(block_timestamp) AS date,
        count()                 AS n_collateral_events,
        uniqExact(group_address) AS n_distinct_groups_acting
    FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs`
    WHERE block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_groups_overview_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_groups_overview_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY date
),

dates AS (
    SELECT date FROM new_groups
    UNION DISTINCT
    SELECT date FROM collateral
)

SELECT
    d.date                                     AS date,
    coalesce(n.n_new_groups, 0)                AS n_new_groups,
    coalesce(c.n_collateral_events, 0)         AS n_collateral_events,
    coalesce(c.n_distinct_groups_acting, 0)    AS n_distinct_groups_acting
FROM dates d
LEFT JOIN new_groups n ON n.date = d.date
LEFT JOIN collateral c ON c.date = d.date