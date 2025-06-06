

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(splitByChar('/', agent_version)[1]) AS client
    FROM `dbt`.`p2p_peers_info`
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`p2p_peers_clients_daily`
    )
  

    GROUP BY 1, 2
)

SELECT
    date
    ,IF(client='','Unknown',client) AS client
    ,COUNT(*) AS value
FROM peers
GROUP BY 1, 2