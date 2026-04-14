


WITH

all_swaps AS (
    SELECT * FROM `dbt`.`stg_pools__dex_trades_uniswap_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_trades_swapr_v3`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_trades_balancer_v2`
    UNION ALL
    SELECT * FROM `dbt`.`stg_pools__dex_trades_balancer_v3`
)

SELECT
    s.block_number,
    s.block_timestamp,
    s.transaction_hash,
    s.log_index,
    s.protocol,
    s.pool_address,
    s.token_bought_address,
    tb.token                                                                         AS token_bought_symbol,
    s.amount_bought_raw,
    s.amount_bought_raw / POWER(10, if(tb.decimals > 0, tb.decimals, 18))           AS amount_bought,
    s.token_sold_address,
    ts.token                                                                         AS token_sold_symbol,
    s.amount_sold_raw,
    s.amount_sold_raw / POWER(10, if(ts.decimals > 0, ts.decimals, 18))             AS amount_sold,
    s.taker
FROM all_swaps s
LEFT JOIN `dbt`.`stg_pools__tokens_meta` tb
    ON  tb.token_address = s.token_bought_address
    AND toDate(s.block_timestamp) >= toDate(tb.date_start)
LEFT JOIN `dbt`.`stg_pools__tokens_meta` ts
    ON  ts.token_address = s.token_sold_address
    AND toDate(s.block_timestamp) >= toDate(ts.date_start)
WHERE s.amount_bought_raw > 0
  AND s.amount_sold_raw   > 0
  
    
  
    
    

   AND 
    toStartOfMonth(toDate(s.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_pools_dex_trades_raw` AS x1
      WHERE 1=1 
    )
    AND toDate(s.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_pools_dex_trades_raw` AS x2
      WHERE 1=1 
    )
  

  