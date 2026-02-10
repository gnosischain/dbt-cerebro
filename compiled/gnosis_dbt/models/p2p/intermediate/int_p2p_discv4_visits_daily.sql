

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
    toStartOfMonth(toDate(visit_ended_at)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_p2p_discv4_visits_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(visit_ended_at) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_p2p_discv4_visits_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
)

SELECT * FROM visits_info