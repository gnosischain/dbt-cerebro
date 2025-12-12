{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:attestations', 'granularity:daily']
    )
}}
SELECT
    date
    ,inclusion_delay
    ,cnt
FROM {{ ref('int_consensus_attestations_daily') }}
ORDER BY date, inclusion_delay
