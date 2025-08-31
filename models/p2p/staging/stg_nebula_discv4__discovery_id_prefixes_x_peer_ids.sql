
{{ 
    config(
        materialized='view',
        tags=['production','nebula_discv4','discovery_id_prefixes_x_peer_ids']
    ) 
}}

WITH

source AS (
  SELECT 
    discovery_id_prefix,
    peer_id
  FROM {{ source('nebula_discv4','discovery_id_prefixes_x_peer_ids') }} 
)

SELECT * FROM source



