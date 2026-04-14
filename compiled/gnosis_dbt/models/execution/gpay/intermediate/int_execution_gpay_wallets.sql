






WITH operational AS (
    SELECT lower(address) AS address
    FROM `dbt`.`gpay_operational_wallets`
),

activated_wallets AS (
    SELECT
        "from"      AS pay_wallet,
        MIN(date)   AS activation_date
    FROM `dbt`.`int_execution_transfers_whitelisted_daily`
    WHERE "to" = '0x4822521e6135cd2599199c83ea35179229a172ee'
      AND date >= toDate('2023-12-01')
      
      
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.activation_date)), -0))
      FROM `dbt`.`int_execution_gpay_wallets` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT 
        
          addDays(max(toDate(x2.activation_date)), -0)
        

      FROM `dbt`.`int_execution_gpay_wallets` AS x2
      WHERE 1=1 
    )
  

      
    GROUP BY "from"
    HAVING pay_wallet NOT IN (SELECT address FROM operational)
    
      AND pay_wallet NOT IN (SELECT address FROM `dbt`.`int_execution_gpay_wallets`)
    
),

safe_setup AS (
    SELECT
        safe_address,
        MIN(block_timestamp) AS creation_time
    FROM `dbt`.`int_execution_safes_owner_events`
    WHERE event_kind = 'safe_setup'
      AND safe_address IN (SELECT pay_wallet FROM activated_wallets)
    GROUP BY safe_address
)

SELECT
    s.safe_address   AS address,
    a.activation_date AS activation_date,
    s.creation_time AS creation_time
FROM safe_setup s
INNER JOIN activated_wallets a
    ON a.pay_wallet = s.safe_address