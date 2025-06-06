


WITH


probelab_agent_version AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,agent_version_semver_str AS version
        ,toInt32(floor(__count)) AS value
    FROM 
        `crawlers_data`.`probelab_agent_semvers_avg_1d` 
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(max_crawl_created_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`probelab_peers_clients_version_daily`
    )
  

)

SELECT
    *
FROM probelab_agent_version 
WHERE date < today()