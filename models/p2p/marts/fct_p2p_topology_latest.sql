{{ 
    config(
        materialized='view',
        pre_hook=[
            "SET enable_dynamic_type = 1"
        ],
        tags=['production','p2p','topology']
    )
}}

WITH

discv4_topology AS (
    SELECT
        date,
        peer_ip,
        peer_discovery_id_prefix,
        peer_client,
        peer_hostname,
        peer_city,
        peer_country,
        peer_org,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(peer_loc, '')), 1)) AS peer_lat,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(peer_loc, '')), 2)) AS peer_lon,
        neighbor_ip,
        neighbor_discovery_id_prefix,
        neighbor_client,
        neighbor_city,
        neighbor_country,
        neighbor_org,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(neighbor_loc, '')), 1)) AS neighbor_lat,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(neighbor_loc, '')), 2)) AS neighbor_lon,
        cnt
    FROM {{ ref('int_p2p_discv4_topology_latest') }}
),

discv5_topology AS (
    SELECT
        date,
        peer_ip,
        peer_discovery_id_prefix,
        peer_client,
        peer_hostname,
        peer_city,
        peer_country,
        peer_org,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(peer_loc, '')), 1)) AS peer_lat,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(peer_loc, '')), 2)) AS peer_lon,
        neighbor_ip,
        neighbor_discovery_id_prefix,
        neighbor_client,
        neighbor_city,
        neighbor_country,
        neighbor_org,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(neighbor_loc, '')), 1)) AS neighbor_lat,
        toFloat64OrNull(arrayElement(splitByChar(',', ifNull(neighbor_loc, '')), 2)) AS neighbor_lon,
        cnt
    FROM {{ ref('int_p2p_discv5_topology_latest') }}
)

SELECT 'DiscV4' AS protocol, * FROM discv4_topology
UNION ALL
SELECT 'DiscV5' AS protocol, * FROM discv5_topology