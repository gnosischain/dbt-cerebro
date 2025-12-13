{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:graffities_labels', 'granularity:in_ranges']
    )
}}


SELECT
    label
    ,graffiti
    ,value
FROM {{ ref('fct_consensus_graffiti_cloud') }}
ORDER BY label DESC, value DESC