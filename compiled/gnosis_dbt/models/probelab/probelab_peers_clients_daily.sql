


WITH


probelab_agent AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,any_value(toInt32(floor(__total))) AS value
    FROM 
        `crawlers_data`.`probelab_agent_semvers_avg_1d` 
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(max_crawl_created_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`probelab_peers_clients_daily`
    )
  

    GROUP BY
        1, 2
)

SELECT
    *
FROM probelab_agent 
WHERE date < today()