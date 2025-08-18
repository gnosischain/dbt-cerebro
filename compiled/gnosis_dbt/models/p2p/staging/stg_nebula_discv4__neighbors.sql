WITH 

source AS (
  SELECT 
    crawl_id,
    crawl_created_at,
    peer_discovery_id_prefix,
    neighbor_discovery_id_prefix,
    error_bits
  FROM `nebula_discv4`.`neighbors` 
)

SELECT * FROM source