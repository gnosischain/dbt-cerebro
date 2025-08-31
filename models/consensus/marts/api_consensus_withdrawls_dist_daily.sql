{{
    config(
        materialized='view',
        tags=["production", "consensus", "withdrawls"]
    )
}}

SELECT
    date
    ,q05
    ,q10
    ,q25
    ,q50
    ,q75
    ,q90
    ,q95
FROM 
    {{ ref('int_consensus_withdrawls_dist_daily') }}
ORDER BY date