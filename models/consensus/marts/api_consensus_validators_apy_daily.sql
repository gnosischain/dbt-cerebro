{{
    config(
        materialized='view',
        tags=["production", "consensus", "validators_apy"]
    )
}}

SELECT 
    date
    ,label
    ,apy
FROM (
    SELECT date, 'Daily' AS label, apy AS apy FROM {{ ref('fct_consensus_validators_apy_daily') }}
    UNION ALL 
    SELECT date, '7DMA' AS label, apy_7dma AS apy FROM {{ ref('fct_consensus_validators_apy_daily') }}
)
ORDER BY date, label