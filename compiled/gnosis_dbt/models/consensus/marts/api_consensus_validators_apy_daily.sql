SELECT 
    date
    ,label
    ,apy
FROM (
    SELECT date, 'Daily' AS label, apy AS apy FROM `dbt`.`fct_consensus_validators_apy_daily`
    UNION ALL 
    SELECT date, '7DMA' AS label, apy_7dma AS apy FROM `dbt`.`fct_consensus_validators_apy_daily`
)
ORDER BY date, label