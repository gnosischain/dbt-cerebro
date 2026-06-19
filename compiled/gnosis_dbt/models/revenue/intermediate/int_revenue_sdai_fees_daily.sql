  






-- Users are EOAs and Safes only. Protocol/token contracts (pools, vaults,
-- the aGnosDAI aToken proxy) hold sDAI but are not fee-paying users; the
-- aToken contract in particular would double count the Aave look-through
-- branch below. The exclusion runs as a single LEFT ANTI JOIN after the
-- union (NOT a NOT IN subquery: the 5.5M-address IN-set materializes via
-- CreatingSetsTransform which cannot spill and OOMs the 10.8 GiB instance
-- under nightly load; joins spill with grace_hash).
WITH non_users AS (
    SELECT address FROM `dbt`.`int_execution_accounts_non_user_contracts`
),

base AS (
    -- Native sDAI balances.
    SELECT date, address AS user, balance_usd
    FROM `dbt`.`int_execution_tokens_balances_daily`
    WHERE date < today()
      AND balance_usd > 0
      AND address IS NOT NULL
      AND symbol = 'sDAI'
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_sdai_fees_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      

    UNION ALL

    -- sDAI held in Aave V3 (aGnosDAI). SparkLend excluded.
    SELECT date, user_address AS user, balance_usd
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date < today()
      AND balance_usd > 0
      AND user_address IS NOT NULL
      AND protocol = 'Aave V3'
      AND symbol = 'sDAI'
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_sdai_fees_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
),

base_users AS (
    SELECT b.*
    FROM base b
    LEFT ANTI JOIN non_users nu ON b.user = nu.address
),

balances AS (
    SELECT date, user, sum(balance_usd) AS balance_usd_sum
    FROM base_users
    GROUP BY date, user
),

rates AS (
    SELECT date, rate
    FROM `dbt`.`int_yields_sdai_rate_daily`
    WHERE rate IS NOT NULL
),

joined AS (
    SELECT
        b.date,
        b.user,
        b.balance_usd_sum,
        r.rate,
        b.balance_usd_sum * r.rate * toFloat64(0.1) AS fees_raw
    FROM balances b
    INNER JOIN rates r USING (date)
)

SELECT
    date,
    user,
    'sDAI' AS symbol,
    round(fees_raw, 8)        AS fees,
    round(balance_usd_sum, 6) AS balance_usd_total
FROM joined