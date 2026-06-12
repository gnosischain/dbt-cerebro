










WITH

-- Per-validator status + balance, scoped to the same active/pending population the
-- old model emitted. balance kept in gwei for the consumers' /POWER(10,9) conversion.
validators AS (
    SELECT
        date
        ,validator_index
        ,status
        ,balance_gwei AS balance
    FROM `dbt`.`int_consensus_validators_snapshots_daily`
    WHERE date < today()
      AND (status LIKE 'active_%' OR status = 'pending_queued')
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -2))
        FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -2)
          

        FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x2
        WHERE 1=1 
  

      )
    
  

    
    
),

-- Spec-bounded per-validator APY (consensus base-reward cap + effective-credit math).
income AS (
    SELECT
        date
        ,validator_index
        ,apy
    FROM `dbt`.`int_consensus_validators_income_daily`
    WHERE date < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -2))
        FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x1
        WHERE 1=1 
  

      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -2)
          

        FROM `dbt`.`int_consensus_validators_per_index_apy_daily` AS x2
        WHERE 1=1 
  

      )
    
  

    
    
)

SELECT
    v.date AS date
    ,v.validator_index AS validator_index
    ,v.status AS status
    ,v.balance AS balance
    ,i.apy AS apy
FROM validators v
INNER JOIN income i
    ON i.date = v.date
    AND i.validator_index = v.validator_index