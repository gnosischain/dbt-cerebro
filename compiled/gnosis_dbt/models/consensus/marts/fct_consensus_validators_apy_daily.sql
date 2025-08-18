SELECT 
    date
    ,balance
    ,rate
    ,avg(rate) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rate_7dma
    ,apy
    ,ROUND((POWER((1+rate_7dma),365) - 1) * 100,2) AS apy_7dma
FROM `dbt`.`int_consensus_validators_apy_daily`
WHERE date > DATE '2021-12-08'