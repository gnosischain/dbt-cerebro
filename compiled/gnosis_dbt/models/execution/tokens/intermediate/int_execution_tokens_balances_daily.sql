









WITH balances AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        balance_raw,
        balance
    FROM `dbt`.`int_execution_tokens_balances_native_daily`
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -2))
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x1
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  


    )
    AND toDate(date) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -2)
        

      FROM `dbt`.`int_execution_tokens_balances_daily` AS x2
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
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
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  

),

prices AS (
    SELECT
        p.date,
        p.symbol,
        p.price
    FROM `dbt`.`int_execution_token_prices_daily` p
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -2))
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x1
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  


    )
    AND toDate(date) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -2)
        

      FROM `dbt`.`int_execution_tokens_balances_daily` AS x2
      WHERE 1=1 
  
  

  
  
    
    
      AND symbol NOT IN (
        
          'aGnoGNO', 
        
          'aGnoWXDAI', 
        
          'aGnosDAI', 
        
          'aGnoUSDC', 
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
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
        
          'aGnoEURe', 
        
          'aGnoUSDCe', 
        
          'spGNO', 
        
          'spUSDT', 
        
          'spUSDC', 
        
          'spUSDC.e'
        
      )
    
  

)

SELECT
    b.date AS date,
    b.token_address AS token_address,
    b.symbol AS symbol,
    b.token_class AS token_class,
    b.address AS address,
    b.balance_raw AS balance_raw,
    b.balance AS balance,
    b.balance * p.price AS balance_usd
FROM balances b
LEFT JOIN prices p
  ON p.date = b.date
 AND upper(p.symbol) = upper(b.symbol)