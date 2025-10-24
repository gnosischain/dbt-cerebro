

WITH

visits_info AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,COUNT(visit_ended_at) AS total_visits
        ,SUM(IF( empty(dial_errors) = 1 OR crawl_error IS NULL, 1, 0)) AS successful_visits
        ,COUNT(DISTINCT crawl_id) AS crawls
    FROM `dbt`.`stg_nebula_discv4__visits`
    WHERE
        toStartOfDay(visit_ended_at) < today()
        AND
        toString(peer_properties.network_id) = '100'
        
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_p2p_discv4_visits_daily` AS t
    )
    AND toStartOfDay(visit_ended_at) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_p2p_discv4_visits_daily` AS t2
    )
  

    GROUP BY 1
)

SELECT * FROM visits_info