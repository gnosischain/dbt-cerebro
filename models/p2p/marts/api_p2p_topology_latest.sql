
{{ 
    config(
        materialized='view',
        tags=['production','p2p','topology']
    )
}}

SELECT
    protocol,
    date,
    peer_discovery_id_prefix,
    peer_client,
    peer_city,
    peer_country,
    peer_org,
    peer_lat,
    peer_lon,
    neighbor_discovery_id_prefix,
    neighbor_client,
    neighbor_city,
    neighbor_country,
    neighbor_org,
    neighbor_lat,
    neighbor_lon,
    cnt
FROM {{ ref('fct_p2p_topology_latest') }}
WHERE peer_lat IS NOT NULL AND neighbor_lat IS NOT NULL
ORDER BY protocol DESC