







WITH balances AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        balance_raw,
        balance
    FROM `dbt`.`int_rpc_state_indexer_token_balances_daily`
    WHERE date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_rpc_state_indexer_token_balances_priced_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
),

prices AS (
    SELECT
        p.date AS date,
        p.symbol AS symbol,
        p.price AS price
    FROM `dbt`.`int_execution_token_prices_daily` AS p
    WHERE p.date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(p.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_rpc_state_indexer_token_balances_priced_daily` AS x1
        WHERE 1=1 
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
FROM balances AS b
LEFT JOIN prices AS p
    ON p.date = b.date
   AND upper(p.symbol) = upper(b.symbol)