{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_active_ongoing', 'granularity:daily']
    )
}}

SELECT 
    date
    ,cnt
FROM {{ ref('int_consensus_validators_status_daily') }}
WHERE status = 'active_ongoing'
ORDER BY date

