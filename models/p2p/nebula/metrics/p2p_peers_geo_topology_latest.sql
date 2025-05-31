{{ 
    config(
        materialized='table',
        pre_hook=[
        "SET enable_dynamic_type = 1"
        ]
    )
}}



WITH

gnosis_peers AS (
    SELECT DISTINCT
        peer_id
        ,peer_properties.ip AS ip
    FROM 
        {{ ref('p2p_peers_info') }}
    WHERE toStartOfDay(visit_ended_at) = DATE '2025-05-25' 
),

gnosis_prefixes AS (
    SELECT 
        t1.discovery_id_prefix
        ,t1.peer_id
        ,t2.ip
    FROM {{ source('nebula','discovery_id_prefixes_x_peer_ids') }} t1
    INNER JOIN 
        gnosis_peers t2
        ON t2.peer_id = t1.peer_id
)

SELECT
    t1.date
    ,t1.peer_ip 
    ,t2.hostname AS peer_hostname
    ,t2.city AS peer_city
    ,t2.country AS peer_country
    ,t2.org AS peer_org
    ,t2.loc AS peer_loc
    ,t1.neighbor_ip 
    ,t3.city AS neighbor_city
    ,t3.country AS neighbor_country
    ,t3.org AS neighbor_org
    ,t3.loc AS neighbor_loc
    ,t1.cnt
FROM (
    SELECT
        toStartOfDay(t1.crawl_created_at) AS date
        ,t2.ip AS peer_ip 
        ,t3.ip AS neighbor_ip 
        ,COUNT(*) AS cnt
    FROM
        {{ source('nebula','neighbors') }} t1 
    INNER JOIN
        gnosis_prefixes t2
        ON t2.discovery_id_prefix = t1.peer_discovery_id_prefix
    INNER JOIN
        gnosis_prefixes t3
        ON t3.discovery_id_prefix = t1.neighbor_discovery_id_prefix
    WHERE toStartOfDay(t1.crawl_created_at) = DATE '2025-05-25' 
    GROUP BY 1, 2, 3
) t1
LEFT JOIN 
    crawlers_data.ipinfo t2 
    ON t2.ip = t1.peer_ip
LEFT JOIN 
    crawlers_data.ipinfo t3 
    ON t3.ip = t1.neighbor_ip