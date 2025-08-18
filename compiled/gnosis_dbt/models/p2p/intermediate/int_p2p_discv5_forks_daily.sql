

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
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_p2p_discv5_forks_daily`
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