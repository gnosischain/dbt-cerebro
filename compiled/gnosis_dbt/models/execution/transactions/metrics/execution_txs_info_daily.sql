


SELECT
    toStartOfDay(block_timestamp) AS date
    ,toString(transaction_type) AS transaction_type
    ,success
    ,COUNT(*) AS n_txs
    ,SUM(COALESCE(gas_used,0)) AS gas_used
    ,CAST(AVG(COALESCE(gas_price,0)) AS Int32) AS gas_price_avg
    ,CAST(median(COALESCE(gas_price,0)) AS Int32) AS gas_price_median
FROM `execution`.`transactions`
WHERE block_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`execution_txs_info_daily`
    )
  

GROUP BY 1, 2, 3