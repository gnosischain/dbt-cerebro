{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier0', 'api:deposits_cnt', 'granularity:latest']
    )
}}

SELECT
    value
    ,change_pct
FROM 
    {{ ref('fct_consensus_info_latest') }}
WHERE
    label = 'deposits_cnt'
   

