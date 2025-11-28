{{ 
    config(
        materialized='view',
        tags=['production','esg','estimated_nodes', 'tier1', 'api: estimated_nodes_d']
    )
}}



SELECT 
    date
    ,baseline_observed_nodes
    ,estimated_nodes
    ,nodes_lower_95
    ,nodes_upper_95
FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}
WHERE toStartOfMonth(date) < toStartOfMonth(today())
ORDER BY date