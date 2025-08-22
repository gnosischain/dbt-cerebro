

WITH

deposists AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount) AS amount
    FROM `dbt`.`stg_consensus__deposits`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_apy_daily`
    )
  

    GROUP BY 1
),

deposists_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount
    FROM `dbt`.`stg_consensus__execution_requests`
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_apy_daily`
    )
  

    GROUP BY 1
),


withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,SUM(amount) AS amount
    FROM `dbt`.`stg_consensus__withdrawals`
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_apy_daily`
    )
  

    GROUP BY 1
),

withdrawals_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
        SUM(toUInt64(JSONExtractString(withdrawals, 'amount'))) AS amount
    FROM `dbt`.`stg_consensus__execution_requests`
    ARRAY JOIN JSONExtractArrayRaw(payload, 'withdrawals') AS withdrawals
    
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_apy_daily`
    )
  

    GROUP BY 1
),

validators AS (
    SELECT
        date,
        balance,
        lagInFrame(balance, 1, balance) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS prev_balance,
        balance - prev_balance AS balance_diff
    FROM (
        SELECT
            toStartOfDay(slot_timestamp) AS date,
            SUM(balance) AS balance
        FROM `dbt`.`stg_consensus__validators`
        
  
    
      
    

    WHERE 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_validators_apy_daily`
    )
  

        GROUP BY 1
    )
)

SELECT 
    t1.date AS date
    ,t1.balance AS balance
    ,t1.balance_diff AS balance_diff_original
    ,COALESCE(t2.amount,0)  AS deposited_amount
    ,COALESCE(t3.amount,0)  AS withdrawaled_amount
    ,t1.balance_diff - COALESCE(t2.amount,0) - COALESCE(t4.amount,0) + COALESCE(t3.amount,0) + COALESCE(t5.amount,0) AS eff_balance_diff
    ,eff_balance_diff/t1.prev_balance AS rate
    ,ROUND((POWER((1+rate),365) - 1) * 100,2) AS apy
FROM validators t1
LEFT JOIN 
    deposists t2
    ON t2.date = t1.date
LEFT JOIN 
    withdrawals t3
    ON t3.date = t1.date
LEFT JOIN 
    deposists_requests t4
    ON t4.date = t1.date
LEFT JOIN 
    withdrawals_requests t5
    ON t5.date = t1.date