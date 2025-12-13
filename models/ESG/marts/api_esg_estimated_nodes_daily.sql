{{ 
    config(
        materialized='view',
        tags=['production','esg', 'tier1', 'api:estimated_nodes', 'granularity:daily']
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