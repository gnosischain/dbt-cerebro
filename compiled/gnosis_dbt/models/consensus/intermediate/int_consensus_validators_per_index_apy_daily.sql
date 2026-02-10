






WITH

withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,validator_index
        ,SUM(amount) AS amount
    FROM `dbt`.`stg_consensus__withdrawals`
    WHERE
        1=1
        
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x2
      WHERE 1=1 
    )
  

        
    GROUP BY 1, 2
),



current_partition AS (
    SELECT 
        max(toStartOfMonth(date)) AS month
        ,max(date)  AS max_date
    FROM `dbt`.`int_consensus_validators_per_index_apy_daily`
    
),
prev_balance AS (
    SELECT 
        t1.validator_index
        ,argMax(t1.balance, t1.date) AS balance
        ,argMax(t1.withdrawaled_amount, t1.date) AS withdrawaled_amount
    FROM `dbt`.`int_consensus_validators_per_index_apy_daily` t1
    CROSS JOIN current_partition t2
    WHERE 
        toStartOfMonth(t1.date) = t2.month
        
        AND 
        t1.date < t2.max_date
        
    GROUP BY t1.validator_index
),


validators AS (
    SELECT
        toStartOfDay(t1.slot_timestamp, 'UTC') AS date,
        t1.validator_index,
        t1.pubkey,
        t1.balance,
        COALESCE(
            lagInFrame(toNullable(t1.balance), 1, NULL) OVER (
                PARTITION BY t1.validator_index
                ORDER BY date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            
                t2.balance
            
        ) AS prev_balance,
        COALESCE(t3.amount,0) AS withdrawaled_amount,
        COALESCE(
            lagInFrame(toNullable(t3.amount), 1, NULL) OVER (
                PARTITION BY t3.validator_index
                ORDER BY date
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            
                t2.withdrawaled_amount
            
        ) AS prev_withdrawaled_amount,
        t1.balance - prev_balance AS balance_diff,
        t1.status AS status
    FROM `dbt`.`stg_consensus__validators` t1
    
    LEFT JOIN prev_balance t2
    ON t2.validator_index = t1.validator_index
    
    LEFT JOIN withdrawals t3
    ON t3.date = toStartOfDay(t1.slot_timestamp, 'UTC')
    AND t3.validator_index = t1.validator_index
    WHERE
        (t1.status LIKE 'active_%' OR t1.status = 'pending_queued')
        
        
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x2
      WHERE 1=1 
    )
  

        
)

SELECT 
    t1.date AS date
    ,t1.validator_index AS validator_index
    ,t1.status AS status
    ,t1.balance AS balance
    ,t1.balance - t1.prev_balance AS balance_diff_original
    ,t1.withdrawaled_amount AS withdrawaled_amount
    ,greatest(
            -- balance + withdrawaled_amount mod 32 mGNO ()
            t1.balance + t1.withdrawaled_amount 
            - MOD(t1.balance + t1.withdrawaled_amount,32000000000) 
            + toUInt64(roundBankers(MOD(t1.balance + t1.withdrawaled_amount, 32000000000) / 32000000000) * 32000000000)

        - (
            -- balance + withdrawaled_amount mod 32 mGNO ()
            t1.prev_balance + t1.prev_withdrawaled_amount 
            - MOD(t1.prev_balance + t1.prev_withdrawaled_amount,32000000000)
            + toUInt64(roundBankers(MOD(t1.prev_balance + t1.prev_withdrawaled_amount, 32000000000) / 32000000000) * 32000000000)
        )
    ,0) AS deposited_amount
    ,balance_diff_original - deposited_amount + withdrawaled_amount AS eff_balance_diff
   -- ,eff_balance_diff/IF(t1.prev_balance=0, deposited_amount, toInt64(t1.prev_balance)) AS rate
    ,eff_balance_diff/(toInt64(t1.prev_balance) + deposited_amount) AS rate
    ,ROUND((POWER((1+rate),365) - 1) * 100,2) AS apy
FROM validators t1