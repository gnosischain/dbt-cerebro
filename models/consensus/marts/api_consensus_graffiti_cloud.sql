{{
    config(
        materialized='view',
        tags=["production", "consensus", "graffiti", 'tier0', 'api: graffiti_cloud']
    )
}}


SELECT
    label
    ,graffiti
    ,value
FROM {{ ref('fct_consensus_graffiti_cloud') }}
ORDER BY label DESC, value DESC