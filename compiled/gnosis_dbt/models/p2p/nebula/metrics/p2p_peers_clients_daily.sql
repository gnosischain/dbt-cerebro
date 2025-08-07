

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,IF(client='','Unknown',client) AS client
        ,IF(client='','Unknown',platform) AS platform
    FROM `dbt`.`p2p_discv5_peers_info`
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`p2p_peers_clients_daily`
    )
  

    GROUP BY 1, 2, 3, 4
)

SELECT
    date
    ,'Clients' AS metric
    ,client AS label
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT
    date
    ,'Platform' AS metric
    ,platform AS label
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2, 3