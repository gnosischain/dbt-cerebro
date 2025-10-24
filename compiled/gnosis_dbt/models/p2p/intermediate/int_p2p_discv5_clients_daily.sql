

WITH

peers AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,argMax(IF(client='','Unknown',client),visit_ended_at) AS client
        ,argMax(platform,visit_ended_at) AS platform
        ,argMax(generic_provider,visit_ended_at) AS generic_provider
        ,argMax(peer_country,visit_ended_at) AS peer_country
    FROM `dbt`.`int_p2p_discv5_peers`
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_p2p_discv5_clients_daily` AS t
    )
    AND toStartOfDay(visit_ended_at) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_p2p_discv5_clients_daily` AS t2
    )
  

    GROUP BY 1, 2
)

SELECT date , 'Clients' AS metric, client AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Platform' AS metric, platform AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Provider' AS metric, generic_provider AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3

UNION ALL 

SELECT date, 'Country' AS metric, peer_country AS label, COUNT(*) AS value FROM peers
GROUP BY 1, 2, 3