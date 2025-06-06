


WITH


probelab_agent_quic AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,quic_support AS quic
        ,__count AS value
    FROM 
        `crawlers_data`.`probelab_quic_support_over_7d` 
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(max_crawl_created_at)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`probelab_peers_clients_quic_daily`
    )
  

)

SELECT
    *
FROM probelab_agent_quic 
WHERE date < today()