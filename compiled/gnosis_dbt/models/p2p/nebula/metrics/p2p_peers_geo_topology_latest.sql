

WITH

-- 1) First CTE: basic peer info
gnosis_peers AS (
    SELECT 
        peer_id,
        cl_fork_name,
        cl_next_fork_name,
        peer_properties.ip AS ip,
        any(splitByChar('/', agent_version)[1]) AS client
    FROM `dbt`.`p2p_peers_info`
    WHERE
        toStartOfDay(visit_ended_at) = today() - INTERVAL 1 DAY
        AND empty(dial_errors) = 1
        AND crawl_error IS NULL
    GROUP BY
        peer_id,
        cl_fork_name,
        cl_next_fork_name,
        ip
),

-- 2) Second CTE: attach discovery_id_prefix to each peer
gnosis_prefixes AS (
    SELECT 
        d.discovery_id_prefix,
        d.peer_id,
        p.ip,
        p.cl_fork_name,
        p.cl_next_fork_name,
        IF(p.client='', 'Unknown', p.client) AS client
    FROM `nebula`.`discovery_id_prefixes_x_peer_ids` AS d
    INNER JOIN gnosis_peers AS p
      ON p.peer_id = d.peer_id
)

SELECT
    t1.date,

    -- Peer columns from the subquery t1
    t1.peer_ip,
    t1.peer_discovery_id_prefix,
    t1.peer_cl_fork_name,
    t1.peer_cl_next_fork_name,
    t1.peer_client,

    -- Geographical info for peer (from crawlers_data.ipinfo)
    peer_info.hostname   AS peer_hostname,
    peer_info.city       AS peer_city,
    peer_info.country    AS peer_country,
    peer_info.org        AS peer_org,
    peer_info.loc        AS peer_loc,

    -- Neighbor columns from subquery t1
    t1.neighbor_ip,
    t1.neighbor_discovery_id_prefix,
    t1.neighbor_cl_fork_name,
    t1.neighbor_cl_next_fork_name,
    t1.neighbor_client,

    -- Geographical info for neighbor (from crawlers_data.ipinfo)
    neighbor_info.city    AS neighbor_city,
    neighbor_info.country AS neighbor_country,
    neighbor_info.org     AS neighbor_org,
    neighbor_info.loc     AS neighbor_loc,

    -- Finally, the count of edges
    t1.cnt
FROM (
    SELECT
        toStartOfDay(n.crawl_created_at) AS date,

        -- “Peer” side of the edge
        peer_p.ip                      AS peer_ip,
        peer_p.discovery_id_prefix     AS peer_discovery_id_prefix,
        peer_p.cl_fork_name            AS peer_cl_fork_name,
        peer_p.cl_next_fork_name       AS peer_cl_next_fork_name,
        peer_p.client                  AS peer_client,

        -- “Neighbor” side of the edge
        neighbor_p.ip                      AS neighbor_ip,
        neighbor_p.discovery_id_prefix     AS neighbor_discovery_id_prefix,
        neighbor_p.cl_fork_name            AS neighbor_cl_fork_name,
        neighbor_p.cl_next_fork_name       AS neighbor_cl_next_fork_name,
        neighbor_p.client                  AS neighbor_client,

        COUNT(*) AS cnt
    FROM `nebula`.`neighbors` AS n

    -- join to get the discovery_prefix + client/fork info for “peer”
    INNER JOIN gnosis_prefixes AS peer_p
      ON peer_p.discovery_id_prefix = n.peer_discovery_id_prefix

    -- join to get the discovery_prefix + client/fork info for “neighbor”
    INNER JOIN gnosis_prefixes AS neighbor_p
      ON neighbor_p.discovery_id_prefix = n.neighbor_discovery_id_prefix

    WHERE
        toStartOfDay(n.crawl_created_at) = today() - INTERVAL 1 DAY

    GROUP BY
        date,
        peer_p.ip,
        peer_p.discovery_id_prefix,
        peer_p.cl_fork_name,
        peer_p.cl_next_fork_name,
        peer_p.client,
        neighbor_p.ip,
        neighbor_p.discovery_id_prefix,
        neighbor_p.cl_fork_name,
        neighbor_p.cl_next_fork_name,
        neighbor_p.client
) AS t1

-- LEFT JOIN to ipinfo for “peer”
LEFT JOIN crawlers_data.ipinfo AS peer_info
  ON peer_info.ip = t1.peer_ip

-- LEFT JOIN to ipinfo for “neighbor”
LEFT JOIN crawlers_data.ipinfo AS neighbor_info
  ON neighbor_info.ip = t1.neighbor_ip