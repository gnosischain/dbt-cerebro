

WITH

source AS (
  SELECT 
    discovery_id_prefix,
    peer_id
  FROM `nebula_discv4`.`discovery_id_prefixes_x_peer_ids` 
)

SELECT * FROM source