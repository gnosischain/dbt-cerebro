


SELECT
    toDate(block_timestamp)    AS date,
    provider,
    pool_address,
    protocol,
    sum(amount_usd)            AS fees_usd
FROM `dbt`.`int_execution_pools_dex_liquidity_events`
WHERE event_type = 'collect'
  AND amount_usd > 0
  AND block_timestamp < today()
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`fct_execution_yields_user_fee_collections_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`fct_execution_yields_user_fee_collections_daily` AS x2
      WHERE 1=1 
    )
  

  
GROUP BY date, provider, pool_address, protocol