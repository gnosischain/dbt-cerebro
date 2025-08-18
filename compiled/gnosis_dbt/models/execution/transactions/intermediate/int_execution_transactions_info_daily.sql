


SELECT
    toStartOfDay(block_timestamp) AS date
    ,toString(transaction_type) AS transaction_type
    ,success
    ,COUNT(*) AS n_txs
    ,SUM(value/POWER(10,18)) AS xdai_value -- xDai units
    ,AVG(value/POWER(10,18)) AS xdai_value_avg -- xDai units
    ,median(value/POWER(10,18)) AS xdai_value_median -- xDai units
    ,SUM(COALESCE(gas_used/POWER(10,9),0)) AS gas_used -- Gas units in Gwei
    ,CAST(AVG(COALESCE(gas_price/POWER(10,9),0)) AS Int32) AS gas_price_avg -- Gas units in Gwei
    ,CAST(median(COALESCE(gas_price/POWER(10,9),0)) AS Int32) AS gas_price_median -- Gas units in Gwei
FROM `dbt`.`stg_execution__transactions`

  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_execution_transactions_info_daily`
    )
  

GROUP BY 1, 2, 3