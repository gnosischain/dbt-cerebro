

WITH

deposists AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount) AS amount
        ,COUNT(*) AS cnt
    FROM `dbt`.`stg_consensus__deposits`
    WHERE
        slot_timestamp < today()
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
),

deposists_requests AS (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount
        ,COUNT() AS cnt
    FROM `dbt`.`stg_consensus__execution_requests`
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    WHERE
        slot_timestamp < today()
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
),

withdrawals AS (
    SELECT 
        toStartOfDay(slot_timestamp) AS date 
        ,SUM(amount) AS amount
        ,COUNT(*) AS cnt
    FROM `dbt`.`stg_consensus__withdrawals`
    WHERE
        slot_timestamp < today()
        
  
    
    

   AND 
    toStartOfMonth(toDate(slot_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(slot_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_consensus_deposits_withdrawals_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
)


SELECT
    label
    ,date
    ,SUM(amount/POWER(10,9)) AS total_amount
    ,SUM(cnt) AS cnt
FROM (
    SELECT 'Deposits' AS label, * FROM deposists
    UNION ALL
    SELECT 'Deposits' AS label, * FROM deposists_requests
    UNION ALL
    SELECT 'Withdrawals' AS label, * FROM withdrawals
)
GROUP BY label, date