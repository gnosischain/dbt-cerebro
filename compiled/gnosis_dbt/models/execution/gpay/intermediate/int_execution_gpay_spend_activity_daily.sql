



WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet FROM `dbt`.`int_execution_gpay_wallets`
),

events_filtered AS (
    SELECT
        toDate(s.block_timestamp) AS date,
        s.spend_account           AS gp_safe_raw,
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
    AND toDate(s.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -0)
        

      FROM `dbt`.`int_execution_gpay_spend_activity_daily` AS x2
      WHERE 1=1 
    )
  

)

SELECT
    e.date,
    e.gp_safe_raw                AS gp_safe,
    count()                      AS spend_count,
    uniqExact(e.spend_asset)     AS distinct_assets,
    uniqExact(e.spend_receiver)  AS distinct_receivers
FROM events_filtered e
INNER JOIN gpay_safes gs ON gs.pay_wallet = e.gp_safe_raw
GROUP BY e.date, e.gp_safe_raw