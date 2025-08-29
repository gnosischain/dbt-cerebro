{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='(date, node_category, client_type)',
        partition_by='toStartOfMonth(date)',
        order_by='(date, node_category, client_type)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}

WITH node_classification AS (
    -- Get total nodes per category from classification model
    SELECT
        date,
        node_category,
        estimated_total_nodes,
        nodes_lower_95,
        nodes_upper_95,
        scaling_factor
    FROM {{ ref('int_esg_node_classification') }}
    {% if is_incremental() %}
        WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Combine client data from both DiscV4 (execution) and DiscV5 (consensus)
combined_client_data AS (
    -- DiscV5 Consensus clients
    SELECT
        date,
        'consensus_' || label AS client_type,
        value AS observed_client_nodes
    FROM {{ ref('int_p2p_discv5_clients_daily') }}
    WHERE metric = 'Clients' 
        AND value > 0
        AND label NOT IN ('Unknown', '')
        {% if is_incremental() %}
            AND date > (SELECT MAX(date) FROM {{ this }}) - INTERVAL 1 DAY
        {% endif %}
    
    UNION ALL
    
    -- DiscV4 Execution clients  
    SELECT
        date,
        'execution_' || label AS client_type,
        value AS observed_client_nodes
    FROM {{ ref('int_p2p_discv4_clients_daily') }}
    WHERE metric = 'Clients'
        AND value > 0  
        AND label NOT IN ('Unknown', '')
        {% if is_incremental() %}
            AND date > (SELECT MAX(date) FROM {{ this }}) - INTERVAL 1 DAY
        {% endif %}
),

-- Calculate client distribution percentages
observed_client_distribution AS (
    SELECT
        date,
        client_type,
        observed_client_nodes,
        round(100.0 * observed_client_nodes / SUM(observed_client_nodes) OVER (PARTITION BY date), 3) AS observed_client_percentage
    FROM combined_client_data
),

-- Apply client distribution to each node category
client_distribution_by_category AS (
    SELECT
        nc.date,
        nc.node_category, 
        ocd.client_type,
        ocd.observed_client_nodes,
        ocd.observed_client_percentage,
        
        -- Calculate client nodes for this category
        -- Each client gets same percentage of each category
        toUInt64(round(
            ocd.observed_client_percentage / 100.0 * nc.estimated_total_nodes
        )) AS estimated_client_nodes,
        
        -- Proportional bounds
        toUInt64(round(
            ocd.observed_client_percentage / 100.0 * nc.nodes_lower_95
        )) AS client_nodes_lower_95,
        
        toUInt64(round(
            ocd.observed_client_percentage / 100.0 * nc.nodes_upper_95
        )) AS client_nodes_upper_95,
        
        -- Client efficiency factors
        CASE 
            -- Consensus client efficiency (relative to baseline)
            WHEN ocd.client_type = 'consensus_Lighthouse' THEN 0.95
            WHEN ocd.client_type = 'consensus_Nimbus' THEN 0.85  
            WHEN ocd.client_type = 'consensus_Teku' THEN 1.15
            WHEN ocd.client_type = 'consensus_Prysm' THEN 1.05
            WHEN ocd.client_type = 'consensus_Lodestar' THEN 1.10
            
            -- Execution client efficiency (relative to baseline)
            WHEN ocd.client_type = 'execution_Erigon' THEN 0.95
            WHEN ocd.client_type = 'execution_Nethermind' THEN 1.00
            WHEN ocd.client_type = 'execution_Besu' THEN 1.02
            WHEN ocd.client_type = 'execution_Geth' THEN 0.98
            
            -- Default for other/unknown clients
            ELSE 1.0
        END AS client_efficiency_factor,
        
        nc.scaling_factor
        
    FROM node_classification nc
    JOIN observed_client_distribution ocd ON nc.date = ocd.date
    WHERE nc.estimated_total_nodes > 0
),

-- Calculate final metrics with rankings
final_client_distribution AS (
    SELECT
        date,
        node_category,
        client_type,
        estimated_client_nodes,
        client_nodes_lower_95,
        client_nodes_upper_95,
        client_efficiency_factor,
        
        -- Percentage within this node category
        round(100.0 * estimated_client_nodes / 
              NULLIF(SUM(estimated_client_nodes) OVER (PARTITION BY date, node_category), 0), 2
        ) AS category_client_percentage,
        
        -- Global percentage across all categories  
        round(100.0 * estimated_client_nodes /
              NULLIF(SUM(estimated_client_nodes) OVER (PARTITION BY date), 0), 2
        ) AS global_client_percentage,
        
        -- Ranking within category
        ROW_NUMBER() OVER (
            PARTITION BY date, node_category 
            ORDER BY estimated_client_nodes DESC, client_type
        ) AS rank_in_category,
        
        -- Global ranking
        ROW_NUMBER() OVER (
            PARTITION BY date 
            ORDER BY estimated_client_nodes DESC, node_category, client_type
        ) AS global_rank
        
    FROM client_distribution_by_category
    WHERE estimated_client_nodes > 0
)

SELECT
    date,
    node_category,
    client_type,
    estimated_client_nodes,
    client_nodes_lower_95,
    client_nodes_upper_95,
    client_efficiency_factor,
    category_client_percentage,
    global_client_percentage,
    rank_in_category,
    global_rank,
    
    -- Metadata
    now() AS calculated_at
    
FROM final_client_distribution
ORDER BY date, node_category, estimated_client_nodes DESC