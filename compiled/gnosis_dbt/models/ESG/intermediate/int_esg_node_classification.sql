


WITH latest_observations AS (
    SELECT 
        toDate(visit_ended_at) as observation_date,
        peer_id,
        argMax(ip, visit_ended_at) AS ip_address,
        argMax(client, visit_ended_at) AS client_type,
        argMax(peer_country, visit_ended_at) AS country_code,
        argMax(generic_provider, visit_ended_at) AS generic_provider,
        argMax(peer_org, visit_ended_at) AS peer_org,
        MAX(visit_ended_at) AS last_seen_that_day
    FROM `dbt`.`int_p2p_discv5_peers`
    WHERE 
        empty(dial_errors) = 1 
        AND crawl_error IS NULL
        AND toStartOfDay(visit_ended_at) < today()
        
            AND toDate(visit_ended_at) > (SELECT MAX(date) FROM `dbt`.`int_esg_node_classification`)
        
    GROUP BY observation_date, peer_id
),

node_categories AS (
    SELECT
        observation_date,
        peer_id,
        ip_address,
        client_type,
        country_code,
        generic_provider,
        peer_org,
        
        -- Classify nodes into operational archetypes based on generic_provider
        CASE
            WHEN generic_provider IN ('AWS', 'Google', 'Azure', 'Oracle Cloud', 'Alibaba Cloud') 
                THEN 'cloud_hosted'
            WHEN generic_provider IN ('DigitalOcean', 'OVHcloud', 'Hetzner', 'Scaleway', 'Linode', 'Vultr', 
                                    'Equinix Metal', 'Hosting/CDN (Other)') 
                THEN 'cloud_hosted'
            WHEN generic_provider = 'Carrier/Transit' 
                THEN 'professional_operator'
            WHEN lower(peer_org) LIKE '%datacenter%' 
                OR lower(peer_org) LIKE '%data center%'
                OR lower(peer_org) LIKE '%hosting%'
                OR lower(peer_org) LIKE '%server%'
                THEN 'professional_operator'
            WHEN generic_provider = 'Public ISP (Home/Office)' 
                THEN 'home_staker'
            WHEN lower(peer_org) LIKE '%telecom%'
                OR lower(peer_org) LIKE '%broadband%'
                OR lower(peer_org) LIKE '%cable%'
                OR lower(peer_org) LIKE '%fiber%'
                OR lower(peer_org) LIKE '%residential%'
                THEN 'home_staker'
            ELSE 'unknown'
        END AS node_category,
        
        CASE
            WHEN generic_provider IN ('AWS', 'Google', 'Azure', 'Oracle Cloud', 'Alibaba Cloud') THEN 0.95
            WHEN generic_provider IN ('DigitalOcean', 'OVHcloud', 'Hetzner', 'Scaleway', 'Linode', 'Vultr') THEN 0.90
            WHEN generic_provider = 'Public ISP (Home/Office)' THEN 0.80
            WHEN generic_provider = 'Hosting/CDN (Other)' THEN 0.75
            WHEN generic_provider = 'Carrier/Transit' THEN 0.70
            WHEN generic_provider = 'Unknown' THEN 0.30
            ELSE 0.50
        END AS classification_confidence
        
    FROM latest_observations
),

daily_distribution AS (
    SELECT
        observation_date AS date,
        node_category,
        COUNT(DISTINCT peer_id) AS observed_nodes,
        AVG(classification_confidence) AS avg_confidence
    FROM node_categories
    GROUP BY observation_date, node_category
),

chao1_data AS (
    SELECT
        p.observation_date,
        p.observed_successful_nodes,
        p.enhanced_total_reachable,
        p.connection_success_rate_pct
    FROM `dbt`.`int_esg_node_population_chao1` p
    WHERE 1=1
        
            AND p.observation_date > (SELECT MAX(date) FROM `dbt`.`int_esg_node_classification`)
        
),

scaled_distribution AS (
    SELECT
        d.date,
        d.node_category,
        d.observed_nodes,
        d.avg_confidence,
        
        CASE 
            WHEN c.observed_successful_nodes > 0 AND c.enhanced_total_reachable > 0 THEN
                toUInt64(greatest(toFloat64(d.observed_nodes), 
                    toFloat64(d.observed_nodes) * c.enhanced_total_reachable / c.observed_successful_nodes))
            ELSE d.observed_nodes
        END AS estimated_total_nodes,
        
        CASE 
            WHEN c.observed_successful_nodes > 0 AND c.enhanced_total_reachable > 0 THEN
                toUInt64(greatest(toFloat64(d.observed_nodes), 
                    toFloat64(d.observed_nodes) * c.enhanced_total_reachable * 0.85 / c.observed_successful_nodes))
            ELSE toUInt64(d.observed_nodes * 0.85)
        END AS nodes_lower_95,
        
        CASE 
            WHEN c.observed_successful_nodes > 0 AND c.enhanced_total_reachable > 0 THEN
                toUInt64(toFloat64(d.observed_nodes) * c.enhanced_total_reachable * 1.15 / c.observed_successful_nodes)
            ELSE toUInt64(d.observed_nodes * 1.15)
        END AS nodes_upper_95,
        
        COALESCE(c.connection_success_rate_pct / 100.0, 0.75) AS sample_coverage,
        
        CASE 
            WHEN c.observed_successful_nodes > 0 AND c.enhanced_total_reachable > 0 THEN
                round(toFloat64(c.enhanced_total_reachable) / c.observed_successful_nodes, 3)
            ELSE 1.0
        END AS scaling_factor
        
    FROM daily_distribution d
    LEFT JOIN chao1_data c ON d.date = c.observation_date
),

geographic_distribution AS (
    SELECT
        observation_date AS date,
        node_category,
        country_code,
        COUNT(DISTINCT peer_id) AS country_observed_nodes
    FROM node_categories
    WHERE country_code IS NOT NULL AND country_code != '' AND country_code != 'Unknown'
    GROUP BY observation_date, node_category, country_code
),

geographic_scaled AS (
    SELECT
        g.date,
        g.node_category,
        g.country_code,
        g.country_observed_nodes,
        
        CASE 
            WHEN s.scaling_factor > 0 THEN
                toUInt64(toFloat64(g.country_observed_nodes) * s.scaling_factor)
            ELSE g.country_observed_nodes
        END AS country_estimated_nodes
        
    FROM geographic_distribution g
    JOIN scaled_distribution s
        ON g.date = s.date AND g.node_category = s.node_category
)

SELECT
    s.date,
    s.node_category,
    s.observed_nodes,
    s.estimated_total_nodes,
    s.nodes_lower_95,
    s.nodes_upper_95,
    s.avg_confidence,
    s.sample_coverage,
    s.scaling_factor,
    
    round(100.0 * s.estimated_total_nodes / 
          SUM(s.estimated_total_nodes) OVER (PARTITION BY s.date), 2) AS category_percentage,
    
    s.estimated_total_nodes - s.observed_nodes AS hidden_nodes_estimated,
    round(100.0 * (s.estimated_total_nodes - s.observed_nodes) / s.estimated_total_nodes, 2) AS hidden_nodes_percentage,
    
    CASE 
        WHEN COUNT(g.country_code) > 0 THEN
            toJSONString(groupArray((g.country_code, g.country_estimated_nodes)))
        ELSE '[]'
    END AS geographic_distribution,
    
    CASE 
        WHEN COUNT(g.country_code) > 0 THEN
            arrayStringConcat(
                arraySlice(
                    arrayMap(x -> tupleElement(x, 1),
                        arraySort(x -> -tupleElement(x, 2),
                            groupArray((g.country_code, g.country_estimated_nodes))
                        )
                    ), 1, 5
                ), ', '
            )
        ELSE 'No geographic data'
    END AS top_countries,
    
    now() AS calculated_at
    
FROM scaled_distribution s
LEFT JOIN geographic_scaled g
    ON s.date = g.date AND s.node_category = g.node_category

GROUP BY 
    s.date, s.node_category, s.observed_nodes, s.estimated_total_nodes,
    s.nodes_lower_95, s.nodes_upper_95, s.avg_confidence, s.sample_coverage, s.scaling_factor

ORDER BY s.date, s.estimated_total_nodes DESC