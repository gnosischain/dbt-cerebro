

WITH

peers_ip AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM `dbt`.`p2p_peers_info`
    WHERE
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`p2p_peers_geo_daily`
    )
  

    GROUP BY 1, 2
)

SELECT
    t1.date
    ,IF(t2.country='','Unknown', t2.country) AS country
    ,COUNT(*) AS cnt
FROM peers_ip t1
LEFT JOIN
    `crawlers_data`.`ipinfo` t2
    ON t1.ip = t2.ip
GROUP BY 1, 2