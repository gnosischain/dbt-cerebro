

-- depends_on: `dbt`.`int_execution_tokens_address_diffs_daily`








WITH deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        address,
        net_delta_raw
    FROM `dbt`.`int_execution_tokens_address_diffs_daily`
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x1
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
      SELECT addDays(max(toDate(x2.date)), -1)
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x2
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

overall_max_date AS (
    SELECT
        least(
            
                today(),
            
            yesterday(),
            (
                SELECT max(toDate(date))
                FROM `dbt`.`int_execution_tokens_address_diffs_daily`
                
            )
        ) AS max_date
),


current_partition AS (
    SELECT 
        max(toStartOfMonth(date)) AS month
        ,max(date)  AS max_date
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date < yesterday()
      
  

      
  
    
    
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
prev_balances AS (
    SELECT 
        t1.token_address,
        t1.symbol,
        t1.token_class,
        t1.address,
        t1.balance_raw
    FROM `dbt`.`int_execution_tokens_balances_daily` t1
    CROSS JOIN current_partition t2
    WHERE 
        t1.date = t2.max_date
        
  

        
  
    
    
      AND t1.symbol NOT IN (
        
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

keys AS (
    SELECT DISTINCT 
        token_address,
        symbol,
        token_class,
        address
    FROM (
        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM prev_balances

        UNION ALL

        SELECT
            token_address,
            symbol,
            token_class,
            address
        FROM deltas
    )
),

calendar AS (
    SELECT
        k.token_address,
        k.symbol,
        k.token_class,
        k.address,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(
        dateDiff('day', cp.max_date, o.max_date)
    ) AS offset
),




balances AS (
    SELECT
        c.date AS date,
        c.token_address AS token_address,
        c.symbol AS symbol,
        c.token_class AS token_class,
        c.address AS address,

        sum(COALESCE(d.net_delta_raw,toInt256(0))) OVER (
            PARTITION BY c.token_address, c.address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.balance_raw, toInt256(0)) 
        
        AS balance_raw
    FROM calendar c
    LEFT JOIN deltas d
      ON d.token_address = c.token_address
     AND d.address       = c.address
     AND d.date          = c.date
    
    LEFT JOIN prev_balances p
      ON p.token_address = c.token_address
     AND p.address       = c.address
    
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
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x1
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
      SELECT addDays(max(toDate(x2.date)), -1)
      FROM `dbt`.`int_execution_tokens_balances_daily` AS x2
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

final AS (
    SELECT
        b.date AS date,
        b.token_address AS token_address,
        b.symbol AS symbol,
        b.token_class AS token_class,
        b.address AS address,
        b.balance_raw AS balance_raw,
        b.balance_raw/POWER(10, t.decimals) AS balance,
        (b.balance_raw/POWER(10, t.decimals)) * p.price AS balance_usd
    FROM balances b
    INNER JOIN `dbt`.`tokens_whitelist` t
      ON lower(t.address) = b.token_address
     AND b.date >= toDate(t.date_start)
     AND (t.date_end IS NULL OR b.date < toDate(t.date_end))
    LEFT JOIN prices p
      ON p.date = b.date
     AND upper(p.symbol) = upper(b.symbol)
    WHERE b.balance_raw != 0
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    balance_raw,
    balance,
    balance_usd
FROM final