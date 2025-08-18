SELECT 
    date
    ,label
    ,apy
FROM (
    SELECT date, 'Daily' AS label, apy AS apy FROM {{ ref('fct_consensus_validators_apy_daily') }}
    UNION ALL 
    SELECT date, '7DMA' AS label, apy_7dma AS apy FROM {{ ref('fct_consensus_validators_apy_daily') }}
)
WHERE date < DATE '2024-04-01'
ORDER BY date, label