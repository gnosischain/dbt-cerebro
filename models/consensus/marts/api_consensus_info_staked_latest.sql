{{
    config(
        materialized='view',
        tags=["production", "consensus", "info", 'tier0', 'api: info_staked_latest']
    )
}}

SELECT
    toUInt32(value) AS value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'Staked'
   

