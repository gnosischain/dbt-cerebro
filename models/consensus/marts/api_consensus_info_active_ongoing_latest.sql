{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:validators_active_ongoing', 'granularity:latest']
    )
}}

SELECT
    value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'active_ongoing'
   

