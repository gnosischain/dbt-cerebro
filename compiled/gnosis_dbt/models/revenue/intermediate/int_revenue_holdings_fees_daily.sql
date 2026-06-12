  
  
  
  






-- Users are EOAs and Safes only. Protocol/token contracts (pools, vaults,
-- aTokens, routers) hold balances but are not fee-paying users; aToken
-- contracts in particular would double count the Aave look-through branch.
-- The exclusion runs as a single LEFT ANTI JOIN after the union (NOT a
-- NOT IN subquery: a 5.5M-address IN-set materializes via
-- CreatingSetsTransform which cannot spill to disk and OOMs the 10.8 GiB
-- instance under nightly load; joins spill with grace_hash).
WITH non_users AS (
    SELECT address FROM `dbt`.`int_execution_accounts_non_user_contracts`
),

base AS (
    -- Native ERC20 balances. svZCHF gets folded into ZCHF below.
    SELECT
        date,
        address AS user,
        multiIf(
            symbol = 'svZCHF', 'ZCHF',
            symbol
        ) AS symbol,
        balance_usd
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date < today()
      AND balance_usd > 0
      AND address IS NOT NULL
      AND symbol IN ('EURe','USDC.e','BRLA','ZCHF','svZCHF')
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_holdings_fees_daily` AS x1
        WHERE 1=1 
      )
    
  

      

    UNION ALL

    -- Aave V3 aToken balances (SparkLend excluded). The `symbol` column
    -- already holds the underlying reserve symbol (e.g. 'EURe' for an
    -- aGnoEURe holder), not the aToken symbol.
    SELECT
        date,
        user_address AS user,
        symbol,
        balance_usd
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date < today()
      AND balance_usd > 0
      AND user_address IS NOT NULL
      AND protocol = 'Aave V3'
      AND symbol IN ('EURe','USDC.e','BRLA','ZCHF')
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_holdings_fees_daily` AS x1
        WHERE 1=1 
      )
    
  

      
),

base_users AS (
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN non_users nu ON b.user = nu.address
),

balances AS (
    SELECT
        date,
        user,
        symbol,
        balance_usd,
        multiIf(
            symbol = 'EURe',   toFloat64(9.6e-06),
            symbol = 'USDC.e', toFloat64(9.6e-06),
            symbol = 'BRLA',   toFloat64(5.61349e-05),
            symbol = 'ZCHF',   toFloat64(1.36646e-05),
            toFloat64(0)
        ) AS daily_rate
    FROM base_users
)

SELECT
    date,
    user,
    symbol,
    round(sum(balance_usd * daily_rate), 8) AS fees,
    round(sum(balance_usd), 6)              AS balance_usd_total
FROM balances
WHERE daily_rate > 0
GROUP BY date, user, symbol