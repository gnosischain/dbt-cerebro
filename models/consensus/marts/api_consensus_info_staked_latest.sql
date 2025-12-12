{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:staked_gno', 'granularity:latest']
    )
}}

SELECT
    toUInt32(value) AS value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'Staked'
   

