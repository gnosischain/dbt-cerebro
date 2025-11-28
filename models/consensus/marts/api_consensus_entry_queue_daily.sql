{{
    config(
        materialized='view',
        tags=["production", "consensus", "entry_queue", 'tier1', 'api: entry_queue_d']
    )
}}

SELECT
    date
    ,validator_count
    ,q05
    ,q10
    ,q25
    ,q50
    ,q75
    ,q90
    ,q95
    ,mean
FROM {{ ref('int_consensus_entry_queue_daily') }}
ORDER BY date ASC
