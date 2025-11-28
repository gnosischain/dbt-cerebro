{{
    config(
        materialized='view',
        tags=["production", "consensus", "attestations", 'tier1', 'api: attestations_d']
    )
}}
SELECT
    date
    ,inclusion_delay
    ,cnt
FROM {{ ref('int_consensus_attestations_daily') }}
ORDER BY date, inclusion_delay
