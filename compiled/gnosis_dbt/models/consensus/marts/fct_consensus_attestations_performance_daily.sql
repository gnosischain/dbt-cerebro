




SELECT
    date
    ,SUM(cnt) AS attestations_total
    ,SUM(inclusion_delay * cnt) / SUM(cnt) AS avg_inclusion_delay
    ,quantileExactWeighted(0.5)(inclusion_delay, cnt) AS p50_inclusion_delay
    ,SUMIf(cnt, inclusion_delay = 1) / SUM(cnt) AS pct_inclusion_distance_1
    ,SUMIf(cnt, inclusion_delay <= 2) / SUM(cnt) AS pct_inclusion_distance_le_2
    ,SUMIf(cnt, inclusion_delay > 1) / SUM(cnt) AS pct_inclusion_distance_gt_1
FROM `dbt`.`int_consensus_attestations_daily`
WHERE 1=1

    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_consensus_attestations_performance_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`fct_consensus_attestations_performance_daily` AS x2
        WHERE 1=1 
      )
    
  


GROUP BY 1