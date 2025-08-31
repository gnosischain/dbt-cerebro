
{{ 
    config(
        materialized='view',
        tags=['production','nebula_discv5','neighbors']
    ) 
}}

WITH 

source AS (
  SELECT 
    crawl_id,
    crawl_created_at,
    peer_discovery_id_prefix,
    neighbor_discovery_id_prefix,
    error_bits
  FROM {{ source('nebula_discv5','neighbors') }} 
)

SELECT * FROM source



