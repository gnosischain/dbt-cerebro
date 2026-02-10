








WITH base AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        lower("from")        AS from_address,
        lower("to")          AS to_address,
        amount_raw,
        transfer_count
    FROM `dbt`.`int_execution_transfers_whitelisted_daily`
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_tokens_transfers_daily` AS x1
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  


    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_tokens_transfers_daily` AS x2
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  


    )
  

      
      
  

      
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  

),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        w.decimals,
        b.from_address,
        b.to_address,
        b.amount_raw,
        b.transfer_count
    FROM base b
    INNER JOIN `dbt`.`tokens_whitelist` w
      ON lower(w.address) = b.token_address
     AND b.date >= toDate(w.date_start)
     AND (w.date_end IS NULL OR b.date < toDate(w.date_end))
),

agg AS (
    SELECT
        date,
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,
        sum(amount_raw / POWER(10, COALESCE(decimals, 18))) AS volume_token,
        sum(transfer_count) AS transfer_count,
        groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
        uniqExact(from_address)                    AS active_senders,
        uniqExact(to_address)                      AS unique_receivers
    FROM with_class
    GROUP BY date, token_address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    volume_token,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM agg
ORDER BY date, token_address