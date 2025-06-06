


WITH


probelab_agent_cloud AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,cloud_provider AS cloud
        ,toInt32(floor(__count)) AS value
    FROM 
        `crawlers_data`.`probelab_cloud_provider_avg_1d` 
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(max_crawl_created_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`probelab_peers_clients_cloud_daily`
    )
  

)

SELECT
    *
FROM probelab_agent_cloud 
WHERE date < today()