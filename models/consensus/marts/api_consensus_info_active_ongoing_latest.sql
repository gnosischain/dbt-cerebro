{{
    config(
        materialized='view',
        tags=["production", "consensus", "info", 'tier0', 'api: info_active_ongoing_latest']
    )
}}

SELECT
    value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'active_ongoing'
   

