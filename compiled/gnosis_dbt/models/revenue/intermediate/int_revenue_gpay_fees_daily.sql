

  
  
  






-- Users are EOAs and Safes only (see int_execution_accounts_non_user_contracts).
-- The exclusion runs as a LEFT ANTI JOIN, not a NOT IN subquery: the
-- 5.5M-address IN-set materializes via CreatingSetsTransform which cannot
-- spill to disk; joins spill with grace_hash.
WITH non_users AS (
    SELECT address FROM `dbt`.`int_execution_accounts_non_user_contracts`
),

transfers AS (
    SELECT
        t.date,
        lower(t."from") AS user,
        t.symbol,
        multiIf(
            t.symbol = 'EURe',   toFloat64(20) / 10000.0,
            t.symbol = 'GBPe',   toFloat64(20) / 10000.0,
            t.symbol = 'USDC.e', toFloat64(100) / 10000.0,
            toFloat64(0)
        ) AS fee_rate,
        sum(toFloat64(t.amount_raw) / pow(10, w.decimals)) AS amount_native
    FROM `dbt`.`int_execution_transfers_whitelisted_daily` t
    INNER JOIN `dbt`.`tokens_whitelist` w
        ON lower(w.address) = t.token_address
       AND t.date >= w.date_start
       AND (w.date_end IS NULL OR t.date < w.date_end)
    WHERE t.date < today()
      AND lower(t."to") = '0x4822521e6135cd2599199c83ea35179229a172ee'
      AND t.symbol IN ('EURe','GBPe','USDC.e')
      AND t.amount_raw IS NOT NULL
      AND t."from" IS NOT NULL
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_gpay_fees_daily` AS x1
        WHERE 1=1 
      )
    
  

      
    GROUP BY t.date, lower(t."from"), t.symbol, fee_rate
),

transfers_users AS (
    SELECT tr.*
    FROM transfers tr
    LEFT ANTI JOIN non_users nu ON tr.user = nu.address
),

prices AS (
    SELECT date, symbol, price
    FROM `dbt`.`int_execution_token_prices_daily`
    WHERE price IS NOT NULL
)

-- user is canonicalized through the June 2026 Safe migration: payments
-- made from a migrated OLD Safe are attributed to its NEW (canonical)
-- Safe so per-user fee series stay continuous. CH LEFT JOIN fills ''
-- on misses, hence the empty-string guard.
SELECT
    tr.date   AS date,
    if(c.canonical_address != '', c.canonical_address, tr.user) AS user,
    tr.symbol AS symbol,
    round(sum(tr.amount_native * tr.fee_rate), 8)           AS fees_native,
    round(sum(tr.amount_native * tr.fee_rate * p.price), 8) AS fees,
    round(sum(tr.amount_native * p.price), 6)               AS volume_usd
FROM transfers_users tr
LEFT JOIN prices p
    ON p.date = tr.date AND p.symbol = tr.symbol
LEFT JOIN `dbt`.`int_execution_gpay_safe_canonical` c
    ON c.address = tr.user
GROUP BY tr.date, if(c.canonical_address != '', c.canonical_address, tr.user), tr.symbol