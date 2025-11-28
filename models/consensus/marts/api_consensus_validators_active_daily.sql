{{
    config(
        materialized='view',
        tags=["production", "consensus", "validators", 'tier1', 'api: validators_active_d']
    )
}}

SELECT 
    date
    ,cnt
FROM {{ ref('int_consensus_validators_status_daily') }}
WHERE status = 'active_ongoing'
ORDER BY date

