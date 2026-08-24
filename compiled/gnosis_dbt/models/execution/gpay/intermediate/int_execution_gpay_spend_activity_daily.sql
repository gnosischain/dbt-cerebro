

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet FROM `dbt`.`int_execution_gpay_wallets`
),

-- Spend.account is a per-card module, not the Safe itself; the bridge
-- resolves it to the Safe the module was enabled on.
account_safes AS (
    SELECT account, safe_address
    FROM `dbt`.`int_execution_gpay_spender_accounts`
    WHERE safe_address IS NOT NULL
),

events_filtered AS (
    SELECT
        toDate(s.block_timestamp) AS date,
        lower(s.spend_account)    AS spend_account,
        s.spend_asset,
        s.spend_receiver
    FROM `dbt`.`int_execution_gpay_spender_events` s
    WHERE s.event_name = 'Spend'
      AND s.spend_account IS NOT NULL
      AND toDate(s.block_timestamp) < today()
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(s.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gpay_spend_activity_daily` AS x1
        WHERE 1=1 
      )
      
    
  

)

SELECT
    e.date,
    a.safe_address               AS gp_safe,
    count()                      AS spend_count,
    uniqExact(e.spend_asset)     AS distinct_assets,
    uniqExact(e.spend_receiver)  AS distinct_receivers
FROM events_filtered e
INNER JOIN account_safes a ON a.account = e.spend_account
INNER JOIN gpay_safes gs ON gs.pay_wallet = a.safe_address
GROUP BY e.date, a.safe_address