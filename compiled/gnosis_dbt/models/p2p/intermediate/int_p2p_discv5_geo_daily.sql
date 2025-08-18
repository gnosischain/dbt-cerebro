

WITH

peers_ip AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,peer_id
        ,any(peer_properties.ip) AS ip
    FROM `dbt`.`int_p2p_discv5_peers`
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        empty(dial_errors) = 1 AND crawl_error IS NULL
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_p2p_discv5_geo_daily`
    )
  

    GROUP BY 1, 2
)

SELECT
    t1.date
    ,IF(t2.country='',NULL,splitByString(',',t2.loc)[1]) AS lat
    ,IF(t2.country='',NULL,splitByString(',',t2.loc)[2]) AS long
    ,IF(t2.country='','Unknown', t2.country) AS country
    ,COUNT(*) AS cnt
FROM peers_ip t1
LEFT JOIN
    `crawlers_data`.`ipinfo` t2
    ON t1.ip = t2.ip
GROUP BY 1, 2, 3, 4