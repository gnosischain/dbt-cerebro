


WITH


probelab_agent_country AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,country
        ,toInt32(floor(__count)) AS value
    FROM 
        `crawlers_data`.`probelab_countries_avg_1d` 
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(max_crawl_created_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`probelab_peers_clients_country_daily`
    )
  

)

SELECT
    *
FROM probelab_agent_country 
WHERE date < today()