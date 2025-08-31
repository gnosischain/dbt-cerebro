{{
    config(
        materialized='view',
        tags=["production", "consensus", "info"]
    )
}}

SELECT
    value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'active_ongoing'
   

