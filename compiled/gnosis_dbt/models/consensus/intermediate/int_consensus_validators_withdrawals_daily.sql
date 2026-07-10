








-- withdrawals_amount_gno is REAL GNO: source amounts are gwei-of-mGNO
-- (32 mGNO = 1 GNO), converted here at the origin via /1e9/32.
-- Consumers must NOT divide by 32 again.
--
-- incremental_strategy resolves to `append` when start_month is set: refresh.py
-- runs validator-index STAGES within each month, and insert_overwrite would make
-- every stage's REPLACE PARTITION wipe the previous stages' rows (verified
-- 2026-07-09: a staged insert_overwrite rebuild left only the 500k-600k stage).
-- Same design as int_consensus_validators_income_daily.
SELECT
    toStartOfDay(slot_timestamp) AS date
    ,validator_index
    ,SUM(amount) / POWER(10, 9) / 32 AS withdrawals_amount_gno
    ,COUNT(*) AS withdrawals_count
FROM `dbt`.`stg_consensus__withdrawals`
WHERE
    slot_timestamp < today()
    
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(slot_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_consensus_validators_withdrawals_daily` AS x1
        WHERE 1=1 
  

      )
      
    
  

    
    
GROUP BY 1, 2