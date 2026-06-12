




WITH

trades AS (
    SELECT *
    FROM `dbt`.`int_execution_pools_dex_trades`
    
    
  
    
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_circles_v2_crc20_prices_raw` AS x1
        WHERE 1=1 
      )
    
  

    
),

wrappers AS (
    SELECT wrapper_address, avatar, circles_type, symbol AS crc20_symbol
    FROM `dbt`.`int_execution_circles_v2_wrapper_tokens`
)

SELECT
    t.block_number,
    t.block_timestamp,
    t.transaction_hash,
    t.log_index,
    t.pool_address,
    t.protocol,
    coalesce(wb.wrapper_address, ws.wrapper_address)        AS crc20_token,
    coalesce(wb.avatar,          ws.avatar)                 AS avatar,
    coalesce(wb.circles_type,    ws.circles_type)           AS circles_type,
    coalesce(wb.crc20_symbol,    ws.crc20_symbol)           AS crc20_symbol,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_address,  t.token_bought_address) AS backing_token,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_symbol,   t.token_bought_symbol)  AS backing_token_symbol,
    if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold)   AS crc_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought) AS backing_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_bought, 0)               AS crc_bought_amount,
    if(ws.wrapper_address IS NOT NULL, t.amount_sold,   0)               AS crc_sold_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought)
        / NULLIF(if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold), 0)
        AS price_in_backing,
    t.amount_usd
FROM trades t
LEFT JOIN wrappers wb ON wb.wrapper_address = t.token_bought_address
LEFT JOIN wrappers ws ON ws.wrapper_address = t.token_sold_address
WHERE (wb.wrapper_address IS NOT NULL OR ws.wrapper_address IS NOT NULL)
  AND t.amount_bought > 1e-4
  AND t.amount_sold   > 1e-4