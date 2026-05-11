

WITH wallets AS (
    SELECT lower(address) AS address, label
    FROM `dbt`.`dao_treasury_wallets`
),

token_holdings AS (
    SELECT
        b.date                  AS date,
        b.address               AS wallet_address,
        w.label                 AS wallet_label,
        b.symbol                AS symbol,
        b.token_class           AS token_class,
        'wallet'                AS position_type,
        ''                      AS protocol,
        b.balance               AS balance,
        b.balance_usd           AS balance_usd
    FROM `dbt`.`int_execution_tokens_balances_daily` b
    INNER JOIN wallets w ON w.address = b.address
    WHERE b.date < today()
      AND b.balance > 0
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(b.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_dao_treasury_holdings_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(b.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_dao_treasury_holdings_daily` AS x2
        WHERE 1=1 
      )
    
  

),

lending_holdings AS (
    SELECT
        l.date                  AS date,
        l.user_address          AS wallet_address,
        w.label                 AS wallet_label,
        l.symbol                AS symbol,
        'LENDING'               AS token_class,
        'lending'               AS position_type,
        l.protocol              AS protocol,
        l.balance               AS balance,
        l.balance_usd           AS balance_usd
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily` l
    INNER JOIN wallets w ON w.address = l.user_address
    WHERE l.date < today()
      AND l.balance > 0
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(l.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_dao_treasury_holdings_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(l.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_dao_treasury_holdings_daily` AS x2
        WHERE 1=1 
      )
    
  

)

SELECT * FROM token_holdings
UNION ALL
SELECT * FROM lending_holdings