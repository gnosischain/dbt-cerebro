








WITH

labels AS (
    -- Use fct_consensus_validators_status_latest (reads stg_consensus__validators_all)
    -- instead of int_consensus_validators_labels — labels filters on balance > 0 and
    -- therefore drops exited validators, causing deposits to any such validator to be
    -- silently skipped.
    SELECT validator_index, lower(pubkey) AS pubkey
    FROM `dbt`.`fct_consensus_validators_status_latest`
    WHERE 1=1
    
),

beacon_deposits AS (
    SELECT
        toStartOfDay(d.slot_timestamp) AS date
        ,l.validator_index AS validator_index
        ,SUM(d.amount) AS amount_gwei
        ,COUNT(*) AS cnt
    FROM `dbt`.`stg_consensus__deposits` d
    INNER JOIN labels l ON l.pubkey = lower(d.pubkey)
    WHERE
        d.slot_timestamp < today()
        
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(d.slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_deposits_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(d.slot_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_consensus_validators_deposits_daily` AS x2
        WHERE 1=1 
  

      )
    
  

        
    GROUP BY 1, 2
),

request_deposits AS (
    SELECT
        toStartOfDay(r.slot_timestamp) AS date
        ,l.validator_index AS validator_index
        ,SUM(toUInt64(JSONExtractString(deposit, 'amount'))) AS amount_gwei
        ,COUNT() AS cnt
    FROM `dbt`.`stg_consensus__execution_requests` r
    ARRAY JOIN JSONExtractArrayRaw(payload, 'deposits') AS deposit
    INNER JOIN labels l ON l.pubkey = lower(JSONExtractString(deposit, 'pubkey'))
    WHERE
        r.slot_timestamp < today()
        
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(r.slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_deposits_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(r.slot_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_consensus_validators_deposits_daily` AS x2
        WHERE 1=1 
  

      )
    
  

        
    GROUP BY 1, 2
)

SELECT
    date
    ,validator_index
    ,SUM(amount_gwei) / POWER(10, 9) AS deposits_amount_gno
    ,SUM(cnt) AS deposits_count
FROM (
    SELECT * FROM beacon_deposits
    UNION ALL
    SELECT * FROM request_deposits
)
GROUP BY 1, 2