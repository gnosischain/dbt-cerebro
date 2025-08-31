{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='(date, node_category, country_code)',
        partition_by='toStartOfMonth(date)',
        order_by='(date, node_category, country_code)',
        tags=['production','esg','nodes']
    )
}}

WITH node_classification AS (
    -- Get node classification data
    SELECT
        date,
        node_category,
        estimated_total_nodes,
        nodes_lower_95,
        nodes_upper_95,
        geographic_distribution
    FROM {{ ref('int_esg_node_classification') }}
    {% if is_incremental() %}
        WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

-- Parse geographic distribution JSON
geographic_parsed AS (
    SELECT
        date,
        node_category,
        estimated_total_nodes,
        nodes_lower_95,
        nodes_upper_95,
        
        -- Parse JSON array of country distributions
        JSONExtract(geographic_distribution, 'Array(Tuple(String, UInt32))') AS country_array
    FROM node_classification
),

geographic_expanded AS (
    SELECT
        date,
        node_category,
        estimated_total_nodes,
        nodes_lower_95, 
        nodes_upper_95,
        
        -- Unpack country data
        arrayJoin(country_array) AS country_tuple,
        tupleElement(country_tuple, 1) AS country_code,
        tupleElement(country_tuple, 2) AS estimated_nodes
        
    FROM geographic_parsed
    WHERE length(country_array) > 0
),

-- Add country metadata from country_codes table
country_enriched AS (
    SELECT
        g.date,
        g.node_category,
        g.country_code,
        g.estimated_nodes,
        
        -- Calculate bounds proportionally
        round(toFloat64(g.estimated_nodes) * g.nodes_lower_95 / g.estimated_total_nodes) AS nodes_lower_95,
        round(toFloat64(g.estimated_nodes) * g.nodes_upper_95 / g.estimated_total_nodes) AS nodes_upper_95,
        
        -- Country percentage within this category
        round(100.0 * g.estimated_nodes / g.estimated_total_nodes, 2) AS category_percentage,
        
        -- Get country metadata from reference table
        COALESCE(cc.name, 'Unknown') AS country_name,
        COALESCE(cc.region, 'Other') AS region,
        cc.`sub-region` AS sub_region,
        cc.`alpha-3` AS country_code_alpha3
        
    FROM geographic_expanded g
    LEFT JOIN {{ ref('stg_crawlers_data__country_codes') }} cc
        ON g.country_code = cc.`alpha-2`
    WHERE g.estimated_nodes > 0  -- Only include countries with nodes
)

SELECT
    date,
    node_category,
    country_code,
    country_name,
    region,
    sub_region,
    country_code_alpha3,
    estimated_nodes AS estimated_total_nodes,
    nodes_lower_95,
    nodes_upper_95,
    category_percentage,
    
    -- Overall percentage across all categories
    round(100.0 * estimated_nodes / SUM(estimated_nodes) OVER (PARTITION BY date), 2) AS global_percentage,
    
    -- Ranking within category
    ROW_NUMBER() OVER (PARTITION BY date, node_category ORDER BY estimated_nodes DESC) AS rank_in_category,
    
    -- Ranking globally
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY estimated_nodes DESC) AS global_rank,
    
    -- Metadata
    now() AS calculated_at
    
FROM country_enriched
ORDER BY date, node_category, estimated_nodes DESC