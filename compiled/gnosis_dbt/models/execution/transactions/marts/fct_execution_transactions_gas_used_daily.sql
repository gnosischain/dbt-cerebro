


SELECT
    toStartOfDay(block_timestamp) AS date
    ,toString(transaction_type) AS transaction_type
    ,success
    ,COUNT(*) AS n_txs
    ,SUM(value/POWER(10,18)) AS xdai_value
    ,AVG(value/POWER(10,18)) AS xdai_value_avg
    ,median(value/POWER(10,18)) AS xdai_value_median
    ,SUM(COALESCE(gas_used,0)) AS gas_used
    ,CAST(AVG(COALESCE(gas_price,0)) AS Int32) AS gas_price_avg
    ,CAST(median(COALESCE(gas_price,0)) AS Int32) AS gas_price_median
FROM `dbt`.`stg_execution__transactions`
WHERE block_timestamp < today()
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`fct_execution_transactions_gas_used_daily`
    )
  

GROUP BY 1, 2, 3