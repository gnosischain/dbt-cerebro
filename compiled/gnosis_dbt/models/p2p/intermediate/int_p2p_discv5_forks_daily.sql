

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,toString(any(cl_fork_name)) AS fork
        ,toString(any(cl_next_fork_name)) AS next_fork
    FROM `dbt`.`int_p2p_discv5_peers`
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
    

   AND 
    toStartOfMonth(toDate(visit_ended_at)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_p2p_discv5_forks_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(visit_ended_at) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_p2p_discv5_forks_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1, 2
)

SELECT
    date
    ,'Current Fork' AS label
    ,fork AS fork
    ,COUNT(*) AS cnt
FROM peers
GROUP BY 1, 2, 3

UNION ALL

SELECT
    date
    ,'Next Fork' AS label
    ,next_fork AS fork
    ,COUNT(*) AS cnt
FROM peers
GROUP BY 1, 2, 3