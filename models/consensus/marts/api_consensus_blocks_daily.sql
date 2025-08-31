{{
    config(
        materialized='view',
        tags=["production", "consensus", "blocks"]
    )
}}


SELECT
    date
    ,label
    ,value
FROM (
    SELECT date, 'produced' AS label, blocks_produced AS value FROM {{ ref('int_consensus_blocks_daily') }}
    UNION ALL 
    SELECT date, 'missed' AS label, blocks_missed AS value FROM {{ ref('int_consensus_blocks_daily') }}
)
ORDER BY date, label